//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

use xcore::settings::{Applies, Fixed, Kind, Presence, Setting, Settings};

/// Whether an accepted message is deleted, unless a Location says.
pub const DELETE_AFTER_FETCH: bool = true;

/// Every setting a Location takes beyond its address, the server's host
/// and port: where a Location logs in.
pub const SETTINGS: &Settings = &Settings {
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a server that stops mid-response is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["imap"];
