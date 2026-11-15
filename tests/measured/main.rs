//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand appends on one session take a millisecond each at most;
//! - a thousand receives on one session take a millisecond a search at
//!   most.
//!
//! Each the millisecond beyond the round's wakes, round by round beside a
//! plain wake, the fastest of five (`xcore::latency::Rounds`).

use std::time::Duration;

use transport::Configured;
use transport::{Login, Transport};
use xcore::latency::Rounds;
use xmip_core_transport_imap::ImapTransport;

/// What an append wakes where a plain wake wakes one: the far end and this
/// end at the APPEND and its continuation, and again at the literal and
/// its tagged OK.
const SEND_WAKES: u32 = 4;

/// What a search wakes: the far end at it, and this end at its answer.
const RECEIVE_WAKES: u32 = 2;

fn login() -> Login {
    Login::new("orders", "se\"cret")
}

/// The far end and the near end that logs in to it.
fn ends() -> (ImapTransport, std::net::TcpListener, ImapTransport) {
    let far_end = ImapTransport::new("127.0.0.1:0", "INBOX", login())
        .timing_out_after(Duration::from_secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near =
        ImapTransport::new(address, "INBOX", login()).timing_out_after(Duration::from_secs(5));
    (far_end, listener, near)
}

#[test]
fn a_thousand_appends_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let (far_end, listener, near) = ends();
    let sender = std::thread::spawn(move || {
        let mut rounds = Rounds::crossing(SEND_WAKES);
        for n in 0..SENDS {
            rounds.round(|| near.send("INBOX", n.to_string().as_bytes()))?;
        }
        Ok::<_, transport::TransportError>(rounds)
    });
    let mut session = far_end
        .accept_one(&listener, Vec::new())
        .expect("accepting");
    for n in 0..SENDS {
        let appended = session.next_append().expect("append").expect("one");
        assert_eq!(appended.bytes, n.to_string().as_bytes());
    }
    let rounds = sender.join().expect("thread").expect("sending");
    rounds.hold("an append");
}

#[test]
fn a_thousand_receives_take_a_millisecond_a_search_at_most() {
    const RECEIVES: usize = 1000;
    let (far_end, listener, near) = ends();
    let receiver = std::thread::spawn(move || {
        let mut rounds = Rounds::crossing(RECEIVE_WAKES);
        for _ in 0..RECEIVES {
            assert!(rounds.round(|| near.receive())?.is_empty());
        }
        Ok::<_, transport::TransportError>(rounds)
    });
    // Served until the near end lets its session go.
    far_end
        .accept_one(&listener, Vec::new())
        .expect("accepting")
        .serve()
        .expect("serving");
    let rounds = receiver.join().expect("thread").expect("collected");
    rounds.hold("a receive, a search");
}
