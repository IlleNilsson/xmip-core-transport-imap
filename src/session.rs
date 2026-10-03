//! The server's side of one session: what a test puts at the far end, and
//! what a Location that hands mail to an IMAP client directly runs.
//!
//! One mailbox, in memory, one client at a time. `LOGIN`, `SELECT`,
//! `SEARCH`, `FETCH n BODY[]` (or `BODY.PEEK[]`), `STORE +FLAGS (\Deleted)`,
//! each by sequence number or by `UID`, `EXPUNGE`, `APPEND`, `NOOP`,
//! `CAPABILITY`, `LOGOUT` — the commands a collector and a depositor use,
//! and no folder tree, no IDLE, no search grammar: every search answers the
//! messages not flagged deleted.

use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use net::ceiling;
use net::{MAX_BODY, read};
use transport::error::{Result, classify, protocol_error};
use transport::socket;
use transport::taken::Taken;

use crate::wire;

/// What the client did, as [`Session::serve`] reports it at the end.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Served {
    /// What the mailbox holds after the session.
    pub mailbox: Vec<Vec<u8>>,
    /// What the client appended, in order.
    pub appended: Vec<Taken>,
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: String,
    mailbox: Vec<Vec<u8>>,
    deleted: Vec<bool>,
    /// Each message's UID, which an expunge does not renumber.
    uids: Vec<u32>,
    appended: Vec<Taken>,
}

impl Session {
    /// Accept one client on `listener`, greet it, and serve `mailbox`.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept(
        listener: &TcpListener,
        mailbox: Vec<Vec<u8>>,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let deleted = vec![false; mailbox.len()];
        let uids = (1..=u32::try_from(mailbox.len()).unwrap_or(u32::MAX)).collect();
        let mut session = Self {
            reader,
            writer,
            peer: peer.to_string(),
            mailbox,
            deleted,
            uids,
            appended: Vec::new(),
        };
        session.write(b"* OK xmip ready\r\n")?;
        Ok(session)
    }

    /// Serve the client until it logs out or drops.
    ///
    /// # Errors
    /// Where the connection broke mid-command.
    pub fn serve(mut self) -> Result<Served> {
        self.serve_until(false)?;
        Ok(self.finish())
    }

    /// Serve the client until it appends a message, and that message:
    /// what a depositor that keeps its session between messages is served
    /// with. `None` where it logged out or dropped first.
    ///
    /// # Errors
    /// Where the connection broke mid-command.
    pub fn next_append(&mut self) -> Result<Option<Taken>> {
        let before = self.appended.len();
        self.serve_until(true)?;
        Ok(self.appended.get(before).cloned())
    }

    /// Serve commands until the client logs out or drops, or — where
    /// `one_append` — has appended one message.
    fn serve_until(&mut self, one_append: bool) -> Result<()> {
        loop {
            let Some(mut line) = read::line(&mut self.reader)? else {
                return Ok(());
            };
            // A command argument sent as a literal — a password with a quote
            // in it — is asked for with `+` and read in. APPEND aside: its
            // literal is the message, read where APPEND is served.
            while !line.to_ascii_uppercase().contains(" APPEND ")
                && let Some(length) = wire::literal_length(&line)?
            {
                ceiling::within(line.len() + length, MAX_BODY, "Xmip reads in one command")?;
                self.write(b"+ go ahead\r\n")?;
                let bytes = wire::read_literal(&mut self.reader, length)?;
                let argument = String::from_utf8(bytes)
                    .map_err(|_| protocol_error("a literal argument that is not UTF-8"))?;
                let rest = wire::line(&mut self.reader)?;
                let open = line.rfind('{').unwrap_or(line.len());
                line.truncate(open);
                line.push_str(&argument);
                line.push_str(&rest);
            }
            let mut words = line.splitn(3, ' ');
            let tag = words.next().unwrap_or("*").to_string();
            let mut verb = words.next().unwrap_or("").to_ascii_uppercase();
            let mut argument = words.next().unwrap_or("").to_string();
            // `UID` names messages by UID rather than sequence number.
            let by_uid = verb == "UID";
            if by_uid {
                let (inner, rest) = argument.split_once(' ').unwrap_or((&argument, ""));
                (verb, argument) = (inner.to_ascii_uppercase(), rest.to_string());
            }
            match verb.as_str() {
                "CAPABILITY" => {
                    self.write(b"* CAPABILITY IMAP4rev1\r\n")?;
                    self.ok(&tag, "capability")?;
                }
                "LOGIN" | "NOOP" => self.ok(&tag, &verb.to_ascii_lowercase())?,
                "SELECT" | "EXAMINE" => {
                    let count = self.deleted.iter().filter(|d| !**d).count();
                    self.write(format!("* {count} EXISTS\r\n").as_bytes())?;
                    self.ok(&tag, "selected")?;
                }
                "SEARCH" => {
                    let numbers: Vec<String> = (1..=self.mailbox.len())
                        .filter(|n| !self.deleted[n - 1])
                        .map(|n| if by_uid { self.uids[n - 1] as usize } else { n })
                        .map(|n| n.to_string())
                        .collect();
                    self.write(format!("* SEARCH {}\r\n", numbers.join(" ")).as_bytes())?;
                    self.ok(&tag, "search done")?;
                }
                "FETCH" => match self.number(&argument, by_uid) {
                    Some(n) => {
                        let body = self.mailbox[n - 1].clone();
                        let uid = self.uids[n - 1];
                        self.write(
                            format!("* {n} FETCH (UID {uid} BODY[] {{{}}}\r\n", body.len())
                                .as_bytes(),
                        )?;
                        self.write(&body)?;
                        self.write(b")\r\n")?;
                        self.ok(&tag, "fetched")?;
                    }
                    None => self.no(&tag, "no such message")?,
                },
                "STORE" => match self.number(&argument, by_uid) {
                    Some(n) => {
                        if argument.contains("\\Deleted") {
                            self.deleted[n - 1] = true;
                        }
                        self.ok(&tag, "stored")?;
                    }
                    None => self.no(&tag, "no such message")?,
                },
                "EXPUNGE" => {
                    let deleted = std::mem::take(&mut self.deleted);
                    let (kept, uids): (Vec<Vec<u8>>, Vec<u32>) = std::mem::take(&mut self.mailbox)
                        .into_iter()
                        .zip(std::mem::take(&mut self.uids))
                        .zip(deleted)
                        .filter(|(_, gone)| !gone)
                        .map(|(kept, _)| kept)
                        .unzip();
                    self.deleted = vec![false; kept.len()];
                    self.mailbox = kept;
                    self.uids = uids;
                    self.ok(&tag, "expunged")?;
                }
                "APPEND" => {
                    self.append(&tag, &argument)?;
                    if one_append {
                        return Ok(());
                    }
                }
                "LOGOUT" => {
                    self.write(b"* BYE\r\n")?;
                    self.ok(&tag, "bye")?;
                    return Ok(());
                }
                _ => self.write(format!("{tag} BAD unknown command\r\n").as_bytes())?,
            }
        }
    }

    fn append(&mut self, tag: &str, argument: &str) -> Result<()> {
        let Some(length) = wire::literal_length(argument)? else {
            return self.no(tag, "append needs a literal");
        };
        self.write(b"+ go ahead\r\n")?;
        let message = wire::read_literal(&mut self.reader, length)?;
        wire::line(&mut self.reader)?;
        self.mailbox.push(message.clone());
        self.deleted.push(false);
        let uid = self.uids.last().map_or(1, |last| last + 1);
        self.uids.push(uid);
        let origin = format!("imap://{}/INBOX/{}", self.peer, self.mailbox.len());
        self.appended.push(Taken::new(origin, message));
        self.ok(tag, "appended")
    }

    fn finish(self) -> Served {
        Served {
            mailbox: self.mailbox,
            appended: self.appended,
        }
    }

    /// The sequence number `argument` names first, by UID where `by_uid`.
    fn number(&self, argument: &str, by_uid: bool) -> Option<usize> {
        let named: u32 = argument.split(' ').next()?.parse().ok()?;
        let n = if by_uid {
            self.uids.iter().position(|uid| *uid == named)? + 1
        } else {
            usize::try_from(named).ok()?
        };
        (n >= 1 && n <= self.mailbox.len() && !self.deleted[n - 1]).then_some(n)
    }

    fn ok(&mut self, tag: &str, text: &str) -> Result<()> {
        self.write(format!("{tag} OK {text}\r\n").as_bytes())
    }

    fn no(&mut self, tag: &str, text: &str) -> Result<()> {
        self.write(format!("{tag} NO {text}\r\n").as_bytes())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| classify("writing a response", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a response", &e))
    }
}
