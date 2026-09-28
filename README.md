# xmip-core-transport-imap

IMAP transport: one message is one Stream, its number in the mailbox beside it; a Location fetches and expunges, or appends, or accepts clients directly. RFC 3501. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location appends on a session logged in once per server and kept (`transport::Pool`); the login is the transport capability's `Login`. `Session::next_append` serves a depositor that keeps its session. Until 2026-09-27 every append logged in and out.

A Receive Location collects on the same kept session, its mailbox selected once (`Client::selecting`): a search in a selected mailbox sees what arrived since. Until 2026-09-28 every receive logged in, selected and logged out.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
