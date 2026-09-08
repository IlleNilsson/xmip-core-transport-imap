//! The client's side of one IMAP session: log in, select, search, fetch,
//! flag deleted, expunge, append, log out.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::wire::{quoted, read, until_tagged};

/// What a Location presents when it logs in.
#[derive(Clone, Debug, Default)]
pub struct Login {
    pub user: String,
    pub password: String,
}

/// One authenticated session.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    next_tag: u32,
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
        let mut exists = 0;
        self.command(&format!("SELECT {}", quoted(mailbox)), "the select", |r| {
            if let Some((count, "EXISTS")) = r.text.split_once(' ') {
                exists = count.parse().unwrap_or(0);
            }
        })?;
        Ok(exists)
    }

    /// The sequence numbers `SEARCH ALL` returns in the selected mailbox.
    ///
    /// # Errors
    /// Where no mailbox is selected.
    pub fn search_all(&mut self) -> Result<Vec<u32>> {
        let mut numbers = Vec::new();
        self.command("SEARCH ALL", "the search", |r| {
            if let Some(rest) = r.text.strip_prefix("SEARCH") {
                numbers.extend(
                    rest.split_whitespace()
                        .filter_map(|n| n.parse::<u32>().ok()),
                );
            }
        })?;
        Ok(numbers)
    }

    /// Message `number`, whole, as `FETCH BODY[]` returns it.
    ///
    /// # Errors
    /// Where there is no such message or the body did not come.
    pub fn fetch(&mut self, number: u32) -> Result<Vec<u8>> {
        let mut body = None;
        self.command(&format!("FETCH {number} BODY[]"), "the fetch", |r| {
            if r.text.contains("FETCH") && r.literal.is_some() {
                body = r.literal;
            }
        })?;
        body.ok_or_else(|| protocol_error("the fetch answered without a body"))
    }

    /// Flag message `number` deleted.
    ///
    /// # Errors
    /// Where there is no such message.
    pub fn delete(&mut self, number: u32) -> Result<()> {
        self.command(
            &format!("STORE {number} +FLAGS (\\Deleted)"),
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

    /// Log out.
    ///
    /// # Errors
    /// Where the connection was already gone.
    pub fn logout(mut self) -> Result<()> {
        self.command("LOGOUT", "the logout", |_| {}).map(|_| ())
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
