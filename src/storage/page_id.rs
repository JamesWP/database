use std::fmt;

/// Typed page identifier — wraps a raw `u32` page number to prevent
/// accidental arithmetic on page IDs and make signatures self-documenting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub(crate) u32);

impl PageId {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for PageId {
    fn from(n: u32) -> Self {
        PageId(n)
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}
