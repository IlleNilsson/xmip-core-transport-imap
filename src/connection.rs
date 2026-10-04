//! The kept IMAP session, shared by the pool and the messages of the
//! receive that searched on it.
//!
//! A receive searches and hands each message back unread, named by its
//! UID. Its body fetches it whole with `BODY.PEEK[]` on the first read —
//! one message in memory at a time, never the mailbox — and its
//! acknowledgement flags it `\Deleted` on `Accepted`. A refusal is not a
//! consumption: on `Refused` the message is left in the mailbox, unseen as
//! the peek left it, and remembered by its UID, so this Location does not
//! collect it again while it lies there unchanged ([`Refused`]); the memory
//! is the node process's, and a node started again collects it once more.
//! The `EXPUNGE` that
//! removes what was accepted is sent once every message of the receive has
//! its verdict, as one was sent at the end of a receive before
//! (`transport::together`), and its failure is the last verdict's. The
//! session is locked for one command and its response, so an append on the
//! same session goes between two.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use transport::body::fetched;
use transport::error::Result;
use transport::pool::Pooled;
use transport::together::together;
use transport::{Arrived, Refused, Verdict};

use crate::client::Client;

/// The messages a Location refused and left in the mailbox, by UID, each
/// with the `UIDVALIDITY` it was refused under.
pub type RefusedMail = Refused<u32, Option<u32>>;

/// A logged-in session, kept by the pool and shared with the messages of
/// the receive that searched on it.
#[derive(Clone)]
pub struct Connection(Arc<Mutex<Client>>);

impl Connection {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self(Arc::new(Mutex::new(client)))
    }

    fn client(&self) -> MutexGuard<'_, Client> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `act` on the session: one command and its response, or a few.
    ///
    /// # Errors
    /// As `act`.
    pub fn with<T>(&self, act: impl FnOnce(&mut Client) -> Result<T>) -> Result<T> {
        act(&mut self.client())
    }

    /// The messages `uids` names in the selected mailbox that `refused`
    /// does not hold, as arrivals from `origin` of each: fetched as the
    /// runtime first reads them, flagged deleted on `Accepted` where
    /// `delete` says, left and remembered in `refused` on `Refused`, and
    /// left on `Failed`. A message never changes under its UID while the
    /// mailbox's `UIDVALIDITY` holds, so that is its stamp.
    #[must_use]
    pub fn arrivals(
        &self,
        uids: Vec<u32>,
        origin: impl Fn(u32) -> String,
        delete: bool,
        refused: &RefusedMail,
    ) -> Vec<Arrived> {
        let validity = self.client().uid_validity();
        let uids = refused.sift(uids, |uid| uid, |_| Some(validity));
        let (deleting, expunging) = (self.clone(), self.clone());
        let listed = uids.clone();
        let acknowledgements = together(
            uids.len(),
            move |at, verdict| match verdict {
                Verdict::Accepted if delete => deleting.with(|client| client.delete(listed[at])),
                Verdict::Accepted | Verdict::Refused(_) | Verdict::Failed => Ok(()),
            },
            // An `EXPUNGE` that fails leaves the messages flagged deleted:
            // no search finds them again, and the next expunge removes them.
            move |verdicts| {
                let flagged = verdicts
                    .iter()
                    .any(|verdict| matches!(verdict, Some(Verdict::Accepted)));
                if delete && flagged {
                    expunging.with(Client::expunge)
                } else {
                    Ok(())
                }
            },
        );
        uids.into_iter()
            .zip(acknowledgements)
            .map(|(uid, acknowledgement)| {
                let connection = self.clone();
                let body = fetched(move || connection.with(|client| client.fetch(uid)));
                let told = refused.remembering(uid, validity, acknowledgement);
                Arrived::new(origin(uid), body, told)
            })
            .collect()
    }
}

impl Pooled for Connection {
    fn usable(&mut self) -> bool {
        self.client().usable()
    }
}
