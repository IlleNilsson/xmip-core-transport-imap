//! RFC 3501 on the wire: tagged commands, untagged `*` responses, the `+`
//! continuation, and the `{n}` literal that carries a message body.

use std::io::BufRead;

use transport::error::{Result, TransportError, classify, protocol_error};

/// One response line, its literal (if the line announced one) read in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// `*`, `+`, or the tag the command was sent with.
    pub tag: String,
    /// The line after the tag, the literal's `{n}` included as written.
    pub text: String,
    /// The bytes a `{n}` at the end of the line announced.
    pub literal: Option<Vec<u8>>,
}

impl Response {
    #[must_use]
    pub fn is_untagged(&self) -> bool {
        self.tag == "*"
    }

    #[must_use]
    pub fn is_continuation(&self) -> bool {
        self.tag == "+"
    }

    /// `OK`, `NO` or `BAD` where this is a tagged completion.
    #[must_use]
    pub fn status(&self) -> &str {
        self.text.split(' ').next().unwrap_or("")
    }
}

/// Read one response. A line ending in `{n}` is followed by `n` bytes and
/// then the rest of the line, which is appended to `text`.
///
/// # Errors
/// A closed connection, or a literal that runs past the end.
pub fn read(reader: &mut impl BufRead) -> Result<Response> {
    let mut line = line(reader)?;
    let mut literal = None;
    if let Some(open) = line.rfind('{')
        && line.ends_with('}')
        && let Ok(length) = line[open + 1..line.len() - 1].parse::<usize>()
    {
        let mut bytes = vec![0u8; length];
        reader
            .read_exact(&mut bytes)
            .map_err(|e| classify("reading a literal", &e))?;
        literal = Some(bytes);
        line.push_str(&self::line(reader)?);
    }
    let (tag, text) = line.split_once(' ').unwrap_or((line.as_str(), ""));
    Ok(Response {
        tag: tag.to_string(),
        text: text.to_string(),
        literal,
    })
}

fn line(reader: &mut impl BufRead) -> Result<String> {
    let mut raw = Vec::new();
    let read = reader
        .read_until(b'\n', &mut raw)
        .map_err(|e| classify("reading a response", &e))?;
    if read == 0 {
        return Err(protocol_error("the peer closed the connection"));
    }
    let text = String::from_utf8_lossy(&raw).into_owned();
    Ok(text.trim_end_matches(['\r', '\n']).to_string())
}

/// Read responses until the one tagged `tag`, handing each untagged one to
/// `each`. The tagged completion must be `OK`.
///
/// # Errors
/// `NO` and `BAD` are permanent; a closed connection is what it is.
pub fn until_tagged(
    reader: &mut impl BufRead,
    tag: &str,
    what: &str,
    mut each: impl FnMut(Response),
) -> Result<Response> {
    loop {
        let response = read(reader)?;
        if response.tag == tag {
            return if response.status() == "OK" {
                Ok(response)
            } else {
                Err(TransportError::permanent(format!(
                    "the server refused {what}: {}",
                    response.text
                )))
            };
        }
        if response.is_untagged() {
            each(response);
        }
    }
}

/// `text` as an IMAP quoted string, or a literal where it cannot be quoted.
#[must_use]
pub fn quoted(text: &str) -> String {
    if text
        .chars()
        .all(|c| c != '"' && c != '\\' && c != '\r' && c != '\n' && !c.is_control())
    {
        format!("\"{text}\"")
    } else {
        format!("{{{}}}\r\n{text}", text.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_read_with_their_literals() {
        let wire = b"* 1 FETCH (BODY[] {11}\r\nhello\r\nbody)\r\nA1 OK done\r\n";
        let mut reader = &wire[..];
        let fetch = read(&mut reader).expect("fetch");
        assert!(fetch.is_untagged());
        assert_eq!(fetch.text, "1 FETCH (BODY[] {11})");
        assert_eq!(fetch.literal.as_deref(), Some(&b"hello\r\nbody"[..]));
        let done = read(&mut reader).expect("done");
        assert_eq!(done.tag, "A1");
        assert_eq!(done.status(), "OK");
        assert!(read(&mut reader).is_err(), "closed");
        let plus = read(&mut &b"+ go ahead\r\n"[..]).expect("continuation");
        assert!(plus.is_continuation());
        assert!(read(&mut &b"* 1 FETCH {99}\r\nshort"[..]).is_err());
    }

    #[test]
    fn until_tagged_collects_untagged_and_judges_the_completion() {
        let wire = b"* SEARCH 1 2\r\n* 2 EXISTS\r\nA2 OK search\r\n";
        let mut seen = Vec::new();
        let done = until_tagged(&mut &wire[..], "A2", "search", |r| seen.push(r.text)).expect("ok");
        assert_eq!(done.status(), "OK");
        assert_eq!(seen, ["SEARCH 1 2", "2 EXISTS"]);
        let refused =
            until_tagged(&mut &b"A3 NO nope\r\n"[..], "A3", "it", |_| {}).expect_err("no");
        assert!(!refused.retryable);
        assert_eq!(quoted("plain"), "\"plain\"");
        assert_eq!(quoted("a\"b"), "{3}\r\na\"b");
    }
}
