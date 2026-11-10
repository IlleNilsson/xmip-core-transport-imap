//! The client's side of one IMAP session: log in, select, search, fetch,
//! flag deleted, expunge, append, log out. A message is named by its UID,
//! which an expunge by another session does not renumber.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::pool::{Pooled, alive};
use transport::{Login, socket};

use crate::wire::{Argument, Response, read, until_tagged};

/// One authenticated session, kept between appends and collections while
/// the server keeps it open, with the mailbox it has selected.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    next_tag: u32,
    selected: Option<String>,
    /// The `UIDVALIDITY` the selected mailbox announced: while it holds, a
    /// UID names one message and that message never changes (RFC 3501
    /// 2.3.1.1).
    uid_validity: Option<u32>,
}

impl Client {
    /// Connect to `server` and log in.
    ///
    /// # Errors
    /// Where the server could not be reached, did not greet, or refused the
    /// login.
    pub fn connect(server: &str, login: &Login, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(server, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            next_tag: 0,
            selected: None,
            uid_validity: None,
        };
        let greeting = read(&mut client.reader)?;
        if !greeting.is_untagged() || greeting.status() == "BYE" {
            return Err(protocol_error("the server did not greet"));
        }
        let arguments = [Argument::of(&login.user), Argument::of(&login.password)];
        client.command("LOGIN", &arguments, "the login", |_| {})?;
        Ok(client)
    }

    /// Select `mailbox`; how many messages it holds.
    ///
    /// # Errors
    /// Where there is no such mailbox.
    pub fn select(&mut self, mailbox: &str) -> Result<u32> {
        let (mut exists, mut validity) = (0, None);
        // A SELECT that fails leaves no mailbox selected (RFC 9051 6.3.2).
        self.selected = None;
        let arguments = [Argument::of(mailbox)];
        self.command("SELECT", &arguments, "the select", |r| {
            if let Some((count, "EXISTS")) = r.text.split_once(' ') {
                exists = count.parse().unwrap_or(0);
            }
            validity = validity.or_else(|| uid_validity(&r.text));
        })?;
        self.selected = Some(mailbox.to_string());
        self.uid_validity = validity;
        Ok(exists)
    }

    /// The `UIDVALIDITY` the selected mailbox announced, `None` where it
    /// announced none.
    #[must_use]
    pub const fn uid_validity(&self) -> Option<u32> {
        self.uid_validity
    }

    /// Select `mailbox` where this session has not already: a session kept
    /// between collections selects its mailbox once, and a search in it
    /// sees what arrived since.
    ///
    /// # Errors
    /// Where there is no such mailbox.
    pub fn selecting(&mut self, mailbox: &str) -> Result<()> {
        if self.selected.as_deref() != Some(mailbox) {
            self.select(mailbox)?;
        }
        Ok(())
    }

    /// The UIDs `UID SEARCH UNDELETED` returns in the selected mailbox: a
    /// message flagged deleted and not yet expunged is already taken.
    ///
    /// # Errors
    /// Where no mailbox is selected.
    pub fn search_undeleted(&mut self) -> Result<Vec<u32>> {
        let mut uids = Vec::new();
        self.command("UID SEARCH UNDELETED", &[], "the search", |r| {
            if let Some(rest) = r.text.strip_prefix("SEARCH") {
                uids.extend(
                    rest.split_whitespace()
                        .filter_map(|n| n.parse::<u32>().ok()),
                );
            }
        })?;
        Ok(uids)
    }

    /// Message `uid`, whole, as `UID FETCH BODY.PEEK[]` returns it: the
    /// peek leaves it unseen, so a refused message stays as it was.
    ///
    /// # Errors
    /// Where there is no such message or the body did not come.
    pub fn fetch(&mut self, uid: u32) -> Result<Vec<u8>> {
        let mut body = None;
        let command = format!("UID FETCH {uid} BODY.PEEK[]");
        self.command(&command, &[], "the fetch", |r| {
            if r.text.contains("FETCH") && r.literal.is_some() {
                body = r.literal;
            }
        })?;
        body.ok_or_else(|| protocol_error("the fetch answered without a body"))
    }

    /// Flag message `uid` deleted.
    ///
    /// # Errors
    /// Where there is no such message.
    pub fn delete(&mut self, uid: u32) -> Result<()> {
        let command = format!("UID STORE {uid} +FLAGS (\\Deleted)");
        self.command(&command, &[], "the delete", |_| {})
            .map(|_| ())
    }

    /// Remove every message flagged deleted.
    ///
    /// # Errors
    /// Where the mailbox is read-only.
    pub fn expunge(&mut self) -> Result<()> {
        self.command("EXPUNGE", &[], "the expunge", |_| {})
            .map(|_| ())
    }

    /// Append `message` to `mailbox`, the message one literal.
    ///
    /// # Errors
    /// Where the server refused the append or never asked for the literal.
    pub fn append(&mut self, mailbox: &str, message: &[u8]) -> Result<()> {
        let arguments = [Argument::of(mailbox), Argument::Literal(message)];
        self.command("APPEND", &arguments, "the append", |_| {})
            .map(|_| ())
    }

    /// Send `command` with `arguments` after it, and read up to its tagged
    /// completion, handing each untagged response to `each`.
    fn command(
        &mut self,
        command: &str,
        arguments: &[Argument<'_>],
        what: &str,
        each: impl FnMut(Response),
    ) -> Result<Response> {
        let tag = self.tag();
        let mut line = format!("{tag} {command}");
        for argument in arguments {
            match argument {
                Argument::Quoted(text) => {
                    line.push_str(" \"");
                    line.push_str(text);
                    line.push('"');
                }
                // The line up to the literal's `{n}` goes first; its bytes
                // follow only once the server asked for them.
                Argument::Literal(bytes) => {
                    line.push_str(" {");
                    line.push_str(&bytes.len().to_string());
                    line.push_str("}\r\n");
                    self.write(line.as_bytes())?;
                    line.clear();
                    self.asked_for_the_literal()?;
                    self.write(bytes)?;
                }
            }
        }
        line.push_str("\r\n");
        self.write(line.as_bytes())?;
        until_tagged(&mut self.reader, &tag, what, each)
    }

    /// Wait for the server's `+` continuation, past any untagged response:
    /// anything else refused the command before its literal.
    fn asked_for_the_literal(&mut self) -> Result<()> {
        loop {
            let response = read(&mut self.reader)?;
            if response.is_continuation() {
                return Ok(());
            }
            if !response.is_untagged() {
                return Err(protocol_error(format!(
                    "the server did not ask for the literal: {}",
                    response.text
                )));
            }
        }
    }

    fn tag(&mut self) -> String {
        self.next_tag += 1;
        format!("X{}", self.next_tag)
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| classify("writing a command", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a command", &e))
    }
}

/// The `n` of a `[UIDVALIDITY n]` response code in `text`.
fn uid_validity(text: &str) -> Option<u32> {
    let (_, rest) = text.split_once("[UIDVALIDITY ")?;
    rest.split_once(']')?.0.trim().parse().ok()
}

impl Pooled for Client {
    /// While the server has not closed the connection — an autologout
    /// after a long idle closes it.
    fn usable(&mut self) -> bool {
        alive(&self.writer)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    use transport::Login;

    use super::{Client, uid_validity};
    use crate::wire;

    #[test]
    fn a_literal_argument_is_sent_only_once_the_server_asked_for_it() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        // The first client is asked for its literal; the second is refused
        // before it, and what it sent after the line is read to its close.
        let far_end = std::thread::spawn(move || {
            [true, false].map(|asks| {
                let (stream, _) = listener.accept().expect("accept");
                let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                let mut writer = stream;
                writer.write_all(b"* OK hi\r\n").expect("greet");
                let line = wire::line(&mut reader).expect("LOGIN");
                let mut after = Vec::new();
                if asks {
                    writer.write_all(b"+ go ahead\r\n").expect("ask");
                    let length = wire::literal_length(&line).expect("length").expect("one");
                    after = wire::read_literal(&mut reader, length).expect("literal");
                    assert_eq!(wire::line(&mut reader).expect("the line's end"), "");
                    writer.write_all(b"X1 OK in\r\n").expect("in");
                } else {
                    writer
                        .write_all(b"X1 NO [AUTHENTICATIONFAILED] refused\r\n")
                        .expect("refuse");
                    reader.read_to_end(&mut after).expect("to the close");
                }
                (line, after)
            })
        });
        // Not seven-bit: a quoted string cannot carry it.
        let login = Login::new("orders", "pässord");
        let timeout = Some(Duration::from_secs(2));
        drop(Client::connect(&address, &login, timeout).expect("logged in"));
        let Err(refused) = Client::connect(&address, &login, timeout) else {
            panic!("logged in");
        };
        assert!(
            refused.message.contains("did not ask for the literal")
                && refused.message.contains("AUTHENTICATIONFAILED"),
            "{refused}"
        );
        assert!(!refused.retryable);
        let line = "X1 LOGIN \"orders\" {8}".to_string();
        let [asked, unasked] = far_end.join().expect("far end");
        assert_eq!(asked, (line.clone(), "pässord".as_bytes().to_vec()));
        assert_eq!(unasked, (line, Vec::new()), "nothing sent unasked");
    }

    #[test]
    fn a_select_reads_the_uid_validity_its_mailbox_announced() {
        assert_eq!(
            uid_validity("OK [UIDVALIDITY 3857529045] UIDs valid"),
            Some(3_857_529_045)
        );
        assert_eq!(uid_validity("OK [UIDNEXT 4] Predicted next UID"), None);
        assert_eq!(uid_validity("12 EXISTS"), None);
    }
}
