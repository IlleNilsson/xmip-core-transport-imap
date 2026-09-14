#![forbid(unsafe_code)]

//! Streams that arrive as mail collected over IMAP, or leave as mail
//! appended to a mailbox. One message is one Stream, its sequence number
//! in the mailbox kept beside it.
//!
//! IMAP is the mailbox that stays on the server: a partner's orders land in
//! a shared box and more than one thing reads it. A Receive Location logs
//! in, selects the mailbox, searches, fetches every message whole and flags
//! what it fetched deleted, expunging at the end; a Send Location appends a
//! message to a mailbox, which is how a Journey files what it concluded
//! where people read it. Either may instead accept clients directly through
//! [`Session`], one client's worth of server over one mailbox.
//!
//! IMAP has artefacts and no locking, so [`Transport::claims`] answers
//! [`NoNativeClaim`], ADR-0024 clause 5: two collectors on one mailbox both
//! see a message until one expunges it, and the Journey's own dedup is what
//! keeps that honest. RFC 3501; IDLE, folders beyond the one selected, and
//! TLS are the next layers.
//!
//! The origin URI carries what the server knew: `imap://server/INBOX/3`.

pub mod client;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Login};
pub use session::{Served, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, NoNativeClaim, ResourceClaim, Transport};

#[derive(Clone)]
pub struct ImapTransport {
    server: String,
    mailbox: String,
    login: Login,
    delete_after_fetch: bool,
    timeout: Option<Duration>,
}

impl ImapTransport {
    /// Collect from, and append to, `mailbox` at `server` as `login`.
    #[must_use]
    pub fn new(server: impl Into<String>, mailbox: &str, login: Login) -> Self {
        Self {
            server: server.into(),
            mailbox: mailbox.to_string(),
            login,
            delete_after_fetch: true,
            timeout: None,
        }
    }

    /// Leave fetched messages in the mailbox rather than deleting them.
    #[must_use]
    pub const fn leaving_mail(mut self) -> Self {
        self.delete_after_fetch = false;
        self
    }

    /// Give up on a server that stops mid-response.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect and log in.
    ///
    /// # Errors
    /// Where the server could not be reached or refused the login.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.server, &self.login, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener, serving `mailbox`.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept_one(&self, listener: &TcpListener, mailbox: Vec<Vec<u8>>) -> Result<Session> {
        Session::accept(listener, mailbox, self.timeout)
    }

    /// Where a target names the server and mailbox itself —
    /// `imap://host:143/INBOX` — or is a mailbox alone on this transport's
    /// server.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match socket::target("imap", target) {
            Some((peer, "")) => (peer, &self.mailbox),
            Some(pair) => pair,
            None => (&self.server, target),
        }
    }
}

impl Transport for ImapTransport {
    fn name(&self) -> &'static str {
        "imap"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Every message in the mailbox, each deleted once fetched unless the
    /// transport was told to leave them.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.select(&self.mailbox)?;
        let mut arrived = Vec::new();
        for number in client.search_all()? {
            let bytes = client.fetch(number)?;
            if self.delete_after_fetch {
                client.delete(number)?;
            }
            arrived.push(Arrived::new(
                format!("imap://{}/{}/{number}", self.server, self.mailbox),
                bytes,
            ));
        }
        if self.delete_after_fetch && !arrived.is_empty() {
            client.expunge()?;
        }
        client.logout()?;
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, mailbox) = self.resolve(target);
        let mut client = Client::connect(server, &self.login, self.timeout)?;
        client.append(mailbox, bytes)?;
        client.logout()
    }

    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(&NoNativeClaim)
    }
}

impl ImapTransport {
    /// Both ends on this machine: an ephemeral local port, an empty INBOX
    /// at the far end, a probe login, the loopback timeout on every read.
    /// The near end appends the payload as one message; the far end takes
    /// what was appended.
    #[must_use]
    pub fn loopback() -> Self {
        let login = Login {
            user: "probe".to_string(),
            password: "probe".to_string(),
        };
        Self::new("127.0.0.1:0", "INBOX", login).timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for ImapTransport {
    fn take_one(&self, listener: &TcpListener) -> Result<Arrived> {
        let mut served = self.accept_one(listener, Vec::new())?.serve()?;
        match served.appended.len() {
            1 => Ok(served.appended.remove(0)),
            count => Err(protocol_error(format!(
                "the client appended {count} messages, not one"
            ))),
        }
    }
}

impl Loopback for ImapTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening::new(self.clone(), listener, address)))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self::new(address, &self.mailbox, self.login.clone())
            .timing_out_after(LOOPBACK_TIMEOUT)
            .send(&self.mailbox, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::edge_payloads;

    fn login() -> Login {
        Login {
            user: "orders".into(),
            password: "se\"cret".into(),
        }
    }

    fn node() -> ImapTransport {
        ImapTransport::new("127.0.0.1:0", "INBOX", login()).timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn the_loopback_appends_one_message_and_takes_it() {
        let message = b"Subject: y\n\n{3}";
        let arrived = ImapTransport::loopback().round(message).expect("round");
        assert_eq!(arrived.bytes, message);
        assert!(arrived.origin_uri.starts_with("imap://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/INBOX/1"));
        let long = vec![0x2a; 100_000];
        assert_eq!(
            ImapTransport::loopback().round(&long).expect("long").bytes,
            long
        );
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let transport = ImapTransport::loopback();
        assert!(transport.ceiling().is_none());
        for (name, bytes) in edge_payloads() {
            assert!(transport.refuses(&bytes).is_none(), "{name}");
            let arrived = transport
                .round(&bytes)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(arrived.bytes, bytes, "{name}");
        }
    }

    #[test]
    fn a_collector_fetches_and_expunges_and_a_depositor_appends() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near = ImapTransport::new(address.clone(), "INBOX", login())
                .timing_out_after(Duration::from_secs(2));
            let collected = near.receive()?;
            near.send(
                &format!("imap://{address}/Sent"),
                b"Subject: filed\r\n\r\nbody\r\n",
            )?;
            Ok::<_, transport::TransportError>(collected)
        });
        let mailbox = vec![b"one\r\n".to_vec(), b"two {3}\r\nxyz".to_vec()];
        let served = far_end
            .accept_one(&listener, mailbox.clone())
            .expect("accepting")
            .serve()
            .expect("serving");
        assert!(served.mailbox.is_empty(), "expunged");
        let served = far_end
            .accept_one(&listener, Vec::new())
            .expect("second")
            .serve()
            .expect("serving");
        assert_eq!(served.appended.len(), 1);
        assert_eq!(served.appended[0].bytes, b"Subject: filed\r\n\r\nbody\r\n");
        assert!(served.appended[0].origin_uri.ends_with("/INBOX/1"));
        let collected = near.join().expect("thread").expect("round trip");
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].bytes, mailbox[0]);
        assert_eq!(collected[1].bytes, mailbox[1]);
        assert!(collected[1].origin_uri.ends_with("/INBOX/2"));
    }

    #[test]
    fn leaving_mail_leaves_it_and_nothing_is_claimed() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            ImapTransport::new(address, "INBOX", login())
                .leaving_mail()
                .timing_out_after(Duration::from_secs(2))
                .receive()
        });
        let served = far_end
            .accept_one(&listener, vec![b"kept".to_vec()])
            .expect("accepting")
            .serve()
            .expect("serving");
        assert_eq!(served.mailbox, vec![b"kept".to_vec()]);
        assert_eq!(near.join().expect("thread").expect("collecting").len(), 1);
        assert!(far_end.claims().is_some());
    }

    #[test]
    fn a_refused_login_is_permanent() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            std::io::Write::write_all(&mut stream, b"* OK hi\r\nX1 NO [AUTHENTICATIONFAILED]\r\n")
                .expect("write");
        });
        let Err(error) = ImapTransport::new(address, "INBOX", login())
            .timing_out_after(Duration::from_secs(2))
            .connect()
        else {
            panic!("connected");
        };
        assert!(!error.retryable);
        assert!(error.message.contains("AUTHENTICATIONFAILED"));
    }
}
