# xmip-core-transport-imap

IMAP transport: one message is one Stream, its number in the mailbox beside it; a Location fetches and expunges, or appends, or accepts clients directly. RFC 3501. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location appends on a session logged in once per server and kept (`transport::Pool`); the login is the transport capability's `Login`. `Session::next_append` serves a depositor that keeps its session. Until 2026-09-27 every append logged in and out.

A Receive Location collects on the same kept session, its mailbox selected once (`Client::selecting`): a search in a selected mailbox sees what arrived since. Until 2026-09-28 every receive logged in, selected and logged out.

## Acknowledgement

A message is consumed only after the runtime's whole receive cycle. A receive searches the mailbox for what is not flagged deleted (`UID SEARCH UNDELETED`) and hands each message back unread, named by its UID; its body is fetched whole with `UID FETCH BODY.PEEK[]` when the runtime first reads it, which leaves it unseen. `Accepted` flags it deleted (`UID STORE +FLAGS (\Deleted)`) unless `delete_after_fetch = false`. `Refused` leaves it in the mailbox, unflagged and unseen: a refusal is not a consumption, and a Stream refused at a transport gate was never written to the Ledger, so the message is the only copy. The Location remembers it by its UID with the mailbox's `UIDVALIDITY` (`transport::Refused`) — while that holds, a UID names one message that never changes (RFC 3501 2.3.1.1) — and does not collect it again while it lies there; the memory is the node process's, so a node started again collects it once more. `Failed` leaves it as it was, and the next receive collects it again. Once every message of a receive has its verdict (`transport::together`), one `EXPUNGE` removes the accepted ones, as one did at the end of a receive before. A message is one IMAP literal, which the estate's IMAP wire reads whole, so its body is whole in memory, one message at a time. Until 2026-10-02 a receive fetched, flagged and expunged every message before handing it back.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
