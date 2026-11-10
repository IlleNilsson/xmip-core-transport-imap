#![forbid(unsafe_code)]

//! Streams that arrive as mail collected over IMAP, or leave as mail
//! appended to a mailbox. One message is one Stream, its sequence number
//! in the mailbox kept beside it.
//!
//! IMAP is the mailbox that stays on the server: a Party's orders land in
//! a shared box and more than one thing reads it. A Receive Location logs
//! in, selects the mailbox, searches, and hands each message back unread:
//! fetched whole with `BODY.PEEK[]` when the runtime first reads it, flagged
//! deleted only when its receive cycle accepted it, and expunged once every
//! message of the receive has its verdict ([`connection`]). A refused
//! message is left in the mailbox, and this Location does not collect it
//! again while it lies there unchanged; a Send Location appends a
//! message to a mailbox, which is how Xmip files what a Message concluded
//! where people read it. Either may instead accept clients directly through
//! [`Session`], one client's worth of server over one mailbox.
//!
//! IMAP has artifacts and no locking, so [`Transport::claims`] answers
//! [`NoNativeClaim`], ADR-0024 clause 5: two collectors on one mailbox both
//! see a message until one expunges it, and the Journey identifier's dedup is what
//! keeps that honest. RFC 3501; IDLE, folders beyond the one selected, and
//! TLS are the next layers.
//!
//! The origin URI carries what the server knew, the message's UID last:
//! `imap://server/INBOX/3`.

pub mod client;
pub mod connection;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::time::Duration;

pub use client::Client;
pub use connection::{Connection, RefusedMail};
use net::Target;
pub use session::{Served, Session};
use transport::ArrivalIdentity;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::taken::Taken;
use transport::{
    Arrived, Configured, Directions, Login, NoNativeClaim, Pool, ResourceClaim, Transport,
};
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

/// Whether an accepted message is deleted, unless a Location says.
const DELETE_AFTER_FETCH: bool = true;

#[derive(Clone)]
pub struct ImapTransport {
    server: String,
    mailbox: String,
    /// Who it logs in as, and the password it presents where its Location
    /// names no secret; where it names one, the secret's password replaces
    /// it at each login ([`transport::secured::logged_in`]).
    login: Login,
    /// The secret its Location names, whose password the login presents,
    /// read at each login (ADR-0019, amendment 2026-10-29).
    secret: Option<transport::Secured>,
    delete_after_fetch: bool,
    timeout: Option<Duration>,
    /// The sessions a send appends on and a receive collects on, logged in
    /// once per server and kept, shared with what a receive handed back; a
    /// collecting one keeps its mailbox selected.
    sessions: Pool<Connection>,
    /// The messages refused and left in the mailbox, shared with the
    /// acknowledgements a receive handed out.
    refused: RefusedMail,
}

impl ImapTransport {
    /// Collect from, and append to, `mailbox` at `server` as `login`, its
    /// password the Location's secret once one is named
    /// ([`Configured::secured_by`]).
    #[must_use]
    pub fn new(server: impl Into<String>, mailbox: &str, login: Login) -> Self {
        Self {
            server: server.into(),
            mailbox: mailbox.to_string(),
            login,
            secret: None,
            delete_after_fetch: DELETE_AFTER_FETCH,
            timeout: None,
            sessions: Pool::new(),
            refused: RefusedMail::default(),
        }
    }

    /// Leave accepted messages in the mailbox rather than deleting them.
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
    /// Where the Location's secret could not be read, or the server could
    /// not be reached or refused the login.
    pub fn connect(&self) -> Result<Client> {
        self.connect_to(&self.server)
    }

    /// Connect to `server` and log in, the password the Location's secret
    /// holds read now where it names one: each new session reads it, a
    /// kept one never again.
    fn connect_to(&self, server: &str) -> Result<Client> {
        let login = transport::secured::logged_in(&self.login, self.secret.as_ref(), "imap")?;
        Client::connect(server, &login, self.timeout)
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
        match Target::under(Self::SCHEMES, target).map(|named| (named.authority(), named.path())) {
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

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a receive searches again what is not yet told")
    }

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Polled("a search of the mailbox returns at once")
    }

    /// Every message in the mailbox not flagged deleted, searched on the
    /// session kept for the server — logged in and the mailbox selected on
    /// the first receive — and handed back unread: each is fetched whole
    /// with `BODY.PEEK[]` when the runtime first reads it; `Accepted` flags
    /// it deleted unless the transport was told to leave mail, `Refused`
    /// leaves it and this Location does not collect it again while it lies
    /// there unchanged, `Failed` leaves it as it was, and the receive's
    /// flagged messages are expunged once every one has its verdict
    /// ([`connection`]).
    fn receive_up_to(&self, most: usize) -> Result<Vec<Arrived>> {
        self.sessions.exchange(
            self.server.as_str(),
            || self.connect().map(Connection::new),
            |connection| {
                let uids = connection.with(|client| {
                    client.selecting(&self.mailbox)?;
                    client.search_undeleted()
                })?;
                let origin = |uid| format!("imap://{}/{}/{uid}", self.server, self.mailbox);
                let delete = self.delete_after_fetch;
                let uids = uids.into_iter().take(most).collect();
                Ok(connection.arrivals(uids, origin, delete, &self.refused))
            },
        )
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Gathered("an APPEND carries the message as one literal")
    }

    /// APPEND on the session kept for the server, logged in on the first
    /// send to it.
    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        let bytes: &[u8] = &outgoing.gather()?;
        let (server, mailbox) = self.resolve(target);
        self.sessions.exchange(
            server,
            || self.connect_to(server).map(Connection::new),
            |connection| connection.with(|client| client.append(mailbox, bytes)),
        )
    }

    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(&NoNativeClaim)
    }
}

impl Configured for ImapTransport {
    /// The address is the server's host and port: where a Location logs in.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "mailbox",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The mailbox a Location collects from, or appends to unless the \
                          target names another.",
                applies: Applies::Both,
            },
            Setting {
                name: "user",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The user a Location logs in as; its password is the Location's \
                          secret.",
                applies: Applies::Both,
            },
            Setting {
                name: "delete_after_fetch",
                kind: Kind::Boolean,
                presence: Presence::Default(Fixed::Boolean(DELETE_AFTER_FETCH)),
                meaning: "Whether a message is deleted once its receive cycle accepted it.",
                applies: Applies::Receive,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a server that stops mid-response is waited on; unbounded \
                          when left out.",
                applies: Applies::Both,
            },
        ],
    };

    /// The schemes a Location's address is entered with to choose this technology.
    const SCHEMES: &'static [&'static str] = &["imap"];

    /// The password is the Location's secret, never a setting; the login is
    /// built without it.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let login = Login::new(settings.text("user"), "");
        let mut transport = Self::new(address, settings.text("mailbox"), login);
        if settings.optional_boolean("delete_after_fetch") == Some(false) {
            transport = transport.leaving_mail();
        }
        if let Some(timeout) = settings.optional_duration("timeout") {
            transport = transport.timing_out_after(timeout);
        }
        Ok(transport)
    }

    /// The password the Location's secret holds, read at each login.
    fn secured_by(mut self, secret: transport::Secured) -> Result<Self> {
        self.secret = Some(secret);
        Ok(self)
    }
}

impl ImapTransport {
    /// Both ends on this machine: an ephemeral local port, an empty INBOX
    /// at the far end, a probe login, the loopback timeout on every read.
    /// The near end appends the payload as one message; the far end takes
    /// what was appended.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "INBOX", Login::new("probe", "probe"))
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for ImapTransport {
    /// The one message the client appends; it keeps its session for the
    /// next.
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
        self.accept_one(listener, Vec::new())?
            .next_append()?
            .ok_or_else(|| protocol_error("the client logged out without appending"))
    }
}

impl Loopback for ImapTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "a mail names its sender in itself, a message identity; the peer is the server",
        )
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A near end of its own at `address`, logging in as this one does.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            secret: self.secret.clone(),
            ..Self::new(address, &self.mailbox, self.login.clone())
        };
        near.timing_out_after(LOOPBACK_TIMEOUT)
            .send(&self.mailbox, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::edge_payloads;

    fn login() -> Login {
        Login::new("orders", "se\"cret")
    }

    #[test]
    fn imap_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(ImapTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            ("mailbox".to_string(), Given::Text("INBOX".to_string())),
            ("user".to_string(), Given::Text("orders".to_string())),
            ("delete_after_fetch".to_string(), Given::Boolean(false)),
        ];
        let built =
            ImapTransport::open("mail.example:143", Applies::Receive, &given).expect("built");
        assert_eq!(built.mailbox, "INBOX");
        assert_eq!(built.login.user, "orders");
        assert!(
            built.login.password.is_empty() && built.secret.is_none(),
            "the password is the Location's secret, none named here"
        );
        assert!(!built.delete_after_fetch);
        let Err(refused) = ImapTransport::open("mail.example:143", Applies::Send, &given[..1])
        else {
            panic!("user is required");
        };
        assert!(refused.message.contains("\"user\""), "{}", refused.message);
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
            let collected = near
                .receive()?
                .into_iter()
                .map(Arrived::taken)
                .collect::<Result<Vec<_>>>()?;
            near.send(
                &format!("imap://{address}/Sent"),
                b"Subject: filed\r\n\r\nbody\r\n",
            )?;
            Ok::<_, transport::TransportError>(collected)
        });
        let mailbox = vec![b"one\r\n".to_vec(), b"two {3}\r\nxyz".to_vec()];
        // One server, so one session collects and appends: one login.
        let served = far_end
            .accept_one(&listener, mailbox.clone())
            .expect("accepting")
            .serve()
            .expect("serving");
        assert_eq!(served.mailbox.len(), 1, "the two collected expunged");
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
    fn a_failed_and_a_refused_message_stay_an_accepted_one_is_expunged() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near = ImapTransport::new(address, "INBOX", login())
                .timing_out_after(Duration::from_secs(2));
            let mut first = near.receive()?;
            assert_eq!(first.len(), 3);
            assert!(first.iter().all(Arrived::defers));
            // The first read and failed, the second refused, the third
            // accepted.
            let (_, mut body, acknowledgement) = first.remove(0).into_parts();
            let mut read = Vec::new();
            std::io::Read::read_to_end(&mut body, &mut read).expect("reading");
            drop(body);
            acknowledgement.acknowledge(transport::Verdict::Failed)?;
            first.remove(0).refused(transport::Refusal::Unacceptable)?;
            let accepted = first.remove(0).taken()?;
            let again = transport::arrived::one_arrival(near.receive()?, "collected again")?;
            let again = again.taken()?;
            let after = near.receive()?.len();
            Ok::<_, transport::TransportError>((read, accepted, again, after))
        });
        let mailbox = vec![
            b"one\r\n".to_vec(),
            b"two\r\n".to_vec(),
            b"three\r\n".to_vec(),
        ];
        let served = far_end
            .accept_one(&listener, mailbox)
            .expect("accepting")
            .serve()
            .expect("serving");
        let (read, accepted, again, after) = near.join().expect("thread").expect("collected");
        assert_eq!(read, b"one\r\n");
        assert_eq!(accepted.bytes, b"three\r\n");
        assert!(accepted.origin_uri.ends_with("/INBOX/3"));
        assert_eq!(
            again.bytes, b"one\r\n",
            "the failed message is collected again, the refused one not"
        );
        assert!(again.origin_uri.ends_with("/INBOX/1"), "by its UID");
        assert_eq!(after, 0, "the refused one still lies there, unchanged");
        assert_eq!(
            served.mailbox,
            vec![b"two\r\n".to_vec()],
            "the refused message is the only copy, and is left in the mailbox"
        );
    }

    #[test]
    fn a_thousand_appends_log_in_once_and_a_session_the_server_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = node().timing_out_after(Duration::from_secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near =
            ImapTransport::new(address, "INBOX", login()).timing_out_after(Duration::from_secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            for n in 0..SENDS {
                sending.send("INBOX", n.to_string().as_bytes())?;
            }
            sending.send("INBOX", b"after the close")
        });
        // One LOGIN for every append: one session accepted.
        let mut session = far_end
            .accept_one(&listener, Vec::new())
            .expect("accepting");
        for n in 0..SENDS {
            let appended = session.next_append().expect("append").expect("one");
            assert_eq!(appended.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end
            .accept_one(&listener, Vec::new())
            .expect("a new session");
        let last = again.next_append().expect("append").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.sessions.opened(), 2);
    }

    #[test]
    fn a_thousand_receives_log_in_once_and_a_session_the_server_closed_is_replaced() {
        const RECEIVES: usize = 1000;
        let far_end = node().timing_out_after(Duration::from_secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near =
            ImapTransport::new(address, "INBOX", login()).timing_out_after(Duration::from_secs(5));
        let (go, going) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            for _ in 0..RECEIVES {
                assert!(near.receive()?.is_empty());
            }
            // An append on the same kept session says the searches are done.
            near.send("INBOX", b"searched")?;
            going.recv().expect("go");
            Ok::<_, transport::TransportError>((near.receive()?, near.sessions.opened()))
        });
        let mut session = far_end
            .accept_one(&listener, Vec::new())
            .expect("accepting");
        let marker = session.next_append().expect("served").expect("the append");
        assert_eq!(marker.bytes, b"searched");
        drop(session);
        go.send(()).expect("went");
        let served = far_end
            .accept_one(&listener, Vec::new())
            .expect("a new session")
            .serve()
            .expect("serving");
        assert_eq!(served.appended, [] as [Taken; 0]);
        let (arrived, opened) = receiver.join().expect("thread").expect("collected");
        assert!(arrived.is_empty());
        assert_eq!(opened, 2);
    }

    #[test]
    fn leaving_mail_leaves_it_and_nothing_is_claimed() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            ImapTransport::new(address, "INBOX", login())
                .leaving_mail()
                .timing_out_after(Duration::from_secs(2))
                .receive()?
                .into_iter()
                .map(Arrived::taken)
                .collect::<Result<Vec<_>>>()
        });
        let served = far_end
            .accept_one(&listener, vec![b"kept".to_vec()])
            .expect("accepting")
            .serve()
            .expect("serving");
        assert_eq!(served.mailbox, vec![b"kept".to_vec()]);
        let collected = near.join().expect("thread").expect("collecting");
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].bytes, b"kept");
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

    /// The `LOGIN` line the next client on `listener` sends, as the server
    /// received it, the login refused once read.
    fn login_offered(listener: &TcpListener) -> std::thread::JoinHandle<String> {
        let listener = listener.try_clone().expect("listener");
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
            let mut writer = stream;
            std::io::Write::write_all(&mut writer, b"* OK hi\r\n").expect("greet");
            let login = wire::line(&mut reader).expect("LOGIN");
            std::io::Write::write_all(&mut writer, b"X1 NO [AUTHENTICATIONFAILED] read\r\n")
                .expect("refuse");
            login
        })
    }

    /// What `act` presented to a far end that refuses every login.
    fn presented(
        listener: &TcpListener,
        act: impl FnOnce() -> Option<transport::TransportError>,
    ) -> String {
        let far_end = login_offered(listener);
        let refused = act().expect("refused by the far end");
        assert!(
            refused.message.contains("AUTHENTICATIONFAILED"),
            "{refused}"
        );
        far_end.join().expect("far end")
    }

    #[test]
    fn the_password_is_the_locations_secret_read_at_each_login() {
        use transport::secured::Kept;
        let kept = std::sync::Arc::new(Kept::default());
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let plain = ImapTransport::new(address.clone(), "INBOX", Login::new("orders", "plain"))
            .timing_out_after(Duration::from_secs(2));
        assert_eq!(
            presented(&listener, || plain.connect().err()),
            "X1 LOGIN \"orders\" \"plain\"",
            "no secret named: the login as it was given"
        );

        let near = plain
            .secured_by(kept.secured("partner-x.imap"))
            .expect("kept");
        // Read before any connection opens: the near end a loopback sends
        // from carries the secret too, and no far end listens at port 9.
        let unset = [
            near.connect().err(),
            near.receive().err(),
            near.send("INBOX", b"UNA").err(),
            near.send_to("127.0.0.1:9", b"UNA").err(),
        ];
        for refused in unset {
            let refused = refused.expect("not set");
            assert!(
                refused.message.contains("'partner-x.imap' is not set"),
                "{refused}"
            );
            assert!(!refused.retryable);
        }

        kept.set(
            "partner-x.imap",
            xcore::SecretKind::Key,
            "-----BEGIN OPENSSH PRIVATE KEY-----",
        );
        let wrong = near.connect().err().expect("a key");
        assert!(
            wrong
                .message
                .contains("is a key, and imap presents a password"),
            "{wrong}"
        );

        let drawn = Kept::drawn();
        kept.set("partner-x.imap", xcore::SecretKind::Password, &drawn);
        let expected = format!("X1 LOGIN \"orders\" \"{drawn}\"");
        assert_eq!(presented(&listener, || near.receive().err()), expected);
        assert_eq!(
            presented(&listener, || near.send("INBOX", b"UNA").err()),
            expected
        );

        let replaced = format!("{}-next", Kept::drawn());
        kept.set("partner-x.imap", xcore::SecretKind::Password, &replaced);
        assert_eq!(
            presented(&listener, || near.send("INBOX", b"UNA").err()),
            format!("X1 LOGIN \"orders\" \"{replaced}\""),
            "a secret replaced reaches the next connection"
        );
    }

    #[test]
    fn a_kept_session_reads_the_secret_no_more_and_a_new_one_reads_it_again() {
        use transport::secured::Kept;
        let kept = std::sync::Arc::new(Kept::default());
        kept.set(
            "partner-x.imap",
            xcore::SecretKind::Password,
            &Kept::drawn(),
        );
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = ImapTransport::new(address, "INBOX", Login::new("orders", ""))
            .timing_out_after(Duration::from_secs(2))
            .secured_by(kept.secured("partner-x.imap"))
            .expect("kept");
        let sending = near.clone();
        let (go, going) = std::sync::mpsc::channel();
        let sender = std::thread::spawn(move || {
            sending.send("INBOX", b"first")?;
            // No password now: read again, it would refuse the next append.
            kept.set(
                "partner-x.imap",
                xcore::SecretKind::Key,
                "-----BEGIN OPENSSH PRIVATE KEY-----",
            );
            sending.send("INBOX", b"second")?;
            going.recv().expect("go");
            Ok::<_, transport::TransportError>(sending.send("INBOX", b"third").err())
        });
        let mut session = far_end
            .accept_one(&listener, Vec::new())
            .expect("accepting");
        for expected in [&b"first"[..], b"second"] {
            let appended = session.next_append().expect("append").expect("one");
            assert_eq!(appended.bytes, expected);
        }
        drop(session);
        go.send(()).expect("went");
        let refused = sender
            .join()
            .expect("thread")
            .expect("both appended on the kept session")
            .expect("the new session read the secret");
        assert!(
            refused
                .message
                .contains("is a key, and imap presents a password"),
            "{refused}"
        );
        assert_eq!(near.sessions.opened(), 1);
    }
}
