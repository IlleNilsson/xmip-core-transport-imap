//! The client's side of one IMAP session: log in, select, search, fetch,
//! flag deleted, expunge, append, log out. A message is named by its UID,
//! which an expunge by another session does not renumber.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::pool::{Pooled, alive};
use transport::{Login, socket};

use crate::wire::{quoted, read, until_tagged};

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
        let command = format!("LOGIN {} {}", quoted(&login.user), quoted(&login.password));
        client.command(&command, "the login", |_| {})?;
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
        self.command(&format!("SELECT {}", quoted(mailbox)), "the select", |r| {
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
        self.command("UID SEARCH UNDELETED", "the search", |r| {
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
        self.command(&format!("UID FETCH {uid} BODY.PEEK[]"), "the fetch", |r| {
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
        self.command(
            &format!("UID STORE {uid} +FLAGS (\\Deleted)"),
            "the delete",
            |_| {},
        )
        .map(|_| ())
    }

    /// Remove every message flagged deleted.
    ///
    /// # Errors
    /// Where the mailbox is read-only.
    pub fn expunge(&mut self) -> Result<()> {
        self.command("EXPUNGE", "the expunge", |_| {}).map(|_| ())
    }

    /// Append `message` to `mailbox`.
    ///
    /// # Errors
    /// Where the server refused the append or never asked for the literal.
    pub fn append(&mut self, mailbox: &str, message: &[u8]) -> Result<()> {
        let tag = self.tag();
        self.write(
            format!("{tag} APPEND {} {{{}}}\r\n", quoted(mailbox), message.len()).as_bytes(),
        )?;
        let go = read(&mut self.reader)?;
        if !go.is_continuation() {
            return Err(protocol_error(format!(
                "the server did not ask for the literal: {}",
                go.text
            )));
        }
        self.write(message)?;
        self.write(b"\r\n")?;
        until_tagged(&mut self.reader, &tag, "the append", |_| {}).map(|_| ())
    }

    fn command(
        &mut self,
        command: &str,
        what: &str,
        each: impl FnMut(crate::wire::Response),
    ) -> Result<crate::wire::Response> {
        let tag = self.tag();
        self.write(format!("{tag} {command}\r\n").as_bytes())?;
        until_tagged(&mut self.reader, &tag, what, each)
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
    use super::uid_validity;

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
