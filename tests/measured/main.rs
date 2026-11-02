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

use std::time::{Duration, Instant};

use transport::{Login, Transport};
use xmip_core_transport_imap::ImapTransport;

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
        let began = Instant::now();
        for n in 0..SENDS {
            near.send("INBOX", n.to_string().as_bytes())?;
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond an append.
        assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
        Ok::<_, transport::TransportError>(())
    });
    let mut session = far_end
        .accept_one(&listener, Vec::new())
        .expect("accepting");
    for n in 0..SENDS {
        let appended = session.next_append().expect("append").expect("one");
        assert_eq!(appended.bytes, n.to_string().as_bytes());
    }
    sender.join().expect("thread").expect("sending");
}

#[test]
fn a_thousand_receives_take_a_millisecond_a_search_at_most() {
    const RECEIVES: usize = 1000;
    let (far_end, listener, near) = ends();
    let receiver = std::thread::spawn(move || {
        let began = Instant::now();
        for _ in 0..RECEIVES {
            assert!(near.receive()?.is_empty());
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a search.
        assert!(took < Duration::from_millis(RECEIVES as u64), "{took:?}");
        Ok::<_, transport::TransportError>(())
    });
    // Served until the near end lets its session go.
    far_end
        .accept_one(&listener, Vec::new())
        .expect("accepting")
        .serve()
        .expect("serving");
    receiver.join().expect("thread").expect("collected");
}
