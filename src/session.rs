//! The server's side of one session: what a test puts at the far end, and
//! what a Location that hands mail to an IMAP client directly runs.
//!
//! One mailbox, in memory, one client at a time. `LOGIN`, `SELECT`,
//! `SEARCH ALL`, `FETCH n BODY[]`, `STORE +FLAGS (\Deleted)`, `EXPUNGE`,
//! `APPEND`, `NOOP`, `CAPABILITY`, `LOGOUT` — the commands a collector and a
//! depositor use, and no folder tree, no IDLE, no search grammar beyond ALL.

use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use net::{MAX_BODY, read};
use transport::error::{Result, classify, protocol_error};
use transport::{Arrived, ceiling, socket};

use crate::wire;

/// What the client did, as [`Session::serve`] reports it at the end.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Served {
    /// What the mailbox holds after the session.
    pub mailbox: Vec<Vec<u8>>,
    /// What the client appended, in order.
    pub appended: Vec<Arrived>,
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: String,
    mailbox: Vec<Vec<u8>>,
    deleted: Vec<bool>,
    appended: Vec<Arrived>,
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
        let mut session = Self {
            reader,
            writer,
            peer: peer.to_string(),
            mailbox,
            deleted,
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
        loop {
            let Some(mut line) = read::line(&mut self.reader)? else {
                return Ok(self.finish());
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
            let verb = words.next().unwrap_or("").to_ascii_uppercase();
            let argument = words.next().unwrap_or("").to_string();
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
                        .map(|n| n.to_string())
                        .collect();
                    self.write(format!("* SEARCH {}\r\n", numbers.join(" ")).as_bytes())?;
                    self.ok(&tag, "search done")?;
                }
                "FETCH" => match self.number(&argument) {
                    Some(n) => {
                        let body = self.mailbox[n - 1].clone();
                        self.write(
                            format!("* {n} FETCH (BODY[] {{{}}}\r\n", body.len()).as_bytes(),
                        )?;
                        self.write(&body)?;
                        self.write(b")\r\n")?;
                        self.ok(&tag, "fetched")?;
                    }
                    None => self.no(&tag, "no such message")?,
                },
                "STORE" => match self.number(&argument) {
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
                    let kept: Vec<Vec<u8>> = std::mem::take(&mut self.mailbox)
                        .into_iter()
                        .zip(deleted)
                        .filter(|(_, gone)| !gone)
                        .map(|(m, _)| m)
                        .collect();
                    self.deleted = vec![false; kept.len()];
                    self.mailbox = kept;
                    self.ok(&tag, "expunged")?;
                }
                "APPEND" => self.append(&tag, &argument)?,
                "LOGOUT" => {
                    self.write(b"* BYE\r\n")?;
                    self.ok(&tag, "bye")?;
                    return Ok(self.finish());
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
        let origin = format!("imap://{}/INBOX/{}", self.peer, self.mailbox.len());
        self.appended.push(Arrived::new(origin, message));
        self.ok(tag, "appended")
    }

    fn finish(self) -> Served {
        Served {
            mailbox: self.mailbox,
            appended: self.appended,
        }
    }

    fn number(&self, argument: &str) -> Option<usize> {
        let n: usize = argument.split(' ').next()?.parse().ok()?;
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
