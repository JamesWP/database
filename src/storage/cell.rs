use serde::{de::Visitor, Deserialize, Serialize};

use crate::engine::scalarvalue::ScalarValue;

pub type Key = Vec<u8>;

#[derive(Debug, Clone)]
pub struct Cell {
    key: Key,
    /// Decoded values — only valid when `continuation.is_none()`.
    values: Vec<ScalarValue>,
    /// Prefix bytes of the CBOR-encoded values — only valid when `continuation.is_some()`.
    /// These are the first `CHUNK_THRESHOLD` bytes of the full CBOR stream; the remainder
    /// lives in the overflow chain pointed to by `continuation`.
    inline_bytes: Vec<u8>,
    continuation: Option<u32>,
}

impl Cell {
    /// Create an inline cell — all values fit on the leaf page, no overflow.
    pub fn new(key: Key, values: Vec<ScalarValue>, continuation: Option<u32>) -> Cell {
        Cell {
            key,
            values,
            inline_bytes: vec![],
            continuation,
        }
    }

    /// Create an overflow cell — `inline_bytes` holds the first portion of the
    /// CBOR-encoded values; the rest lives in the overflow chain starting at
    /// `continuation`.
    pub fn new_overflow(key: Key, inline_bytes: Vec<u8>, continuation: u32) -> Cell {
        Cell {
            key,
            values: vec![],
            inline_bytes,
            continuation: Some(continuation),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn values(&self) -> &[ScalarValue] {
        &self.values
    }

    /// Returns the inline byte prefix for overflow cells.
    /// Empty for inline cells.
    pub fn inline_bytes(&self) -> &[u8] {
        &self.inline_bytes
    }

    pub fn continuation(&self) -> Option<u32> {
        self.continuation
    }
}

impl Serialize for Cell {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let len = if self.continuation.is_some() { 3 } else { 2 };
        let mut tup = serializer.serialize_tuple(len)?;
        tup.serialize_element(serde_bytes::Bytes::new(&self.key))?;
        if self.continuation.is_some() {
            // Overflow cell: second element is a CBOR byte-string (the inline prefix).
            tup.serialize_element(serde_bytes::Bytes::new(&self.inline_bytes))?;
            tup.serialize_element(&self.continuation.unwrap())?;
        } else {
            // Inline cell: second element is a CBOR array of typed values.
            tup.serialize_element(&self.values)?;
        }
        tup.end()
    }
}

/// Second element of a serialized Cell is either a typed-value array (inline)
/// or a raw byte-string (overflow prefix).
#[derive(Deserialize)]
#[serde(untagged)]
enum ValuesPayload {
    Array(Vec<ScalarValue>),
    Bytes(#[serde(with = "serde_bytes")] Vec<u8>),
}

struct CellDeserializeVisitor;
impl<'de> Visitor<'de> for CellDeserializeVisitor {
    type Value = (Vec<u8>, Vec<ScalarValue>, Vec<u8>, Option<u32>);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of two or three values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let key: serde_bytes::ByteBuf = seq.next_element()?.unwrap();
        let payload: ValuesPayload = seq
            .next_element()?
            .unwrap_or(ValuesPayload::Array(vec![]));
        let continuation: Option<u32> = seq.next_element()?;

        let (values, inline_bytes) = match payload {
            ValuesPayload::Array(v) => (v, vec![]),
            ValuesPayload::Bytes(b) => (vec![], b),
        };

        Ok((key.into_vec(), values, inline_bytes, continuation))
    }
}

impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let cell_deserialize_visitor = CellDeserializeVisitor {};
        let (key, values, inline_bytes, continuation) =
            deserializer.deserialize_seq(cell_deserialize_visitor)?;
        Ok(Self {
            key,
            values,
            inline_bytes,
            continuation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_cbor_roundtrip() {
        let key = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39]; // 12345u64.to_be_bytes()
        let values = vec![ScalarValue::String("test value".to_string())];
        let cell = Cell::new(key.clone(), values.clone(), None);

        // Serialize with CBOR
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&cell, &mut cbor).unwrap();

        // Deserialize from CBOR
        let decoded: Cell = ciborium::de::from_reader(&cbor[..]).unwrap();

        assert_eq!(decoded.key(), key.as_slice());
        assert_eq!(decoded.values(), values.as_slice());
        assert_eq!(decoded.inline_bytes(), &[] as &[u8]);
        assert_eq!(decoded.continuation(), None);
    }

    #[test]
    fn test_cell_cbor_roundtrip_overflow() {
        let key = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63]; // 99u64.to_be_bytes()
        let inline_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let continuation = 42u32;
        let cell = Cell::new_overflow(key.clone(), inline_bytes.clone(), continuation);

        // Serialize with CBOR
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&cell, &mut cbor).unwrap();

        // Deserialize from CBOR
        let decoded: Cell = ciborium::de::from_reader(&cbor[..]).unwrap();

        assert_eq!(decoded.key(), key.as_slice());
        assert_eq!(decoded.values(), &[] as &[ScalarValue]);
        assert_eq!(decoded.inline_bytes(), inline_bytes.as_slice());
        assert_eq!(decoded.continuation(), Some(continuation));
    }

    #[test]
    fn test_cell_variable_length_keys() {
        // Short key
        let short_key = vec![0x01];
        let cell = Cell::new(short_key.clone(), vec![ScalarValue::Integer(1)], None);
        assert_eq!(cell.key(), short_key.as_slice());

        // Long key (1KB)
        let long_key = vec![0xABu8; 1024];
        let cell = Cell::new(long_key.clone(), vec![ScalarValue::Integer(1)], None);
        assert_eq!(cell.key(), long_key.as_slice());

        // Empty key
        let empty_key = vec![];
        let cell = Cell::new(empty_key.clone(), vec![ScalarValue::Integer(1)], None);
        assert_eq!(cell.key(), empty_key.as_slice());
    }
}
