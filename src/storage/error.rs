use std::fmt;

use super::node::NodePage;

/// Storage-layer error type.
#[derive(Debug)]
pub enum Error {
    /// File I/O failure.
    Io(std::io::Error),
    /// Encoded node exceeded page size; ownership of the node is returned so
    /// the caller can split it without a re-fetch or clone.
    PageFull(NodePage),
    /// CBOR decode failure.
    Decode(String),
    /// Unknown format version or bad magic number.
    FormatError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::PageFull(_) => write!(f, "encoded node exceeds page size"),
            Error::Decode(msg) => write!(f, "decode error: {msg}"),
            Error::FormatError(msg) => write!(f, "format error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
