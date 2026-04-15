use serde::{de::Visitor, Deserialize, Serialize};

use crate::engine::scalarvalue::ScalarValue;

pub type Key = Vec<u8>;

#[derive(Debug, Clone)]
pub struct Cell {
    key: Key,
    values: Vec<ScalarValue>,
    continuation: Option<u32>,
}

impl Cell {
    pub fn new(key: Key, values: Vec<ScalarValue>, continuation: Option<u32>) -> Cell {
        Cell {
            key,
            values,
            continuation,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn values(&self) -> &[ScalarValue] {
        &self.values
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
        tup.serialize_element(&self.values)?;
        if let Some(cont) = self.continuation {
            tup.serialize_element(&cont)?;
        }
        tup.end()
    }
}

struct CellDeserializeVisitor;
impl<'de> Visitor<'de> for CellDeserializeVisitor {
    type Value = (Vec<u8>, Vec<ScalarValue>, Option<u32>);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of two or three values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let key: serde_bytes::ByteBuf = seq.next_element()?.unwrap();
        let values: Vec<ScalarValue> = seq.next_element()?.unwrap_or_default();
        let continuation = seq.next_element()?;

        Ok((key.into_vec(), values, continuation))
    }
}

impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let cell_deserialize_visitor = CellDeserializeVisitor {};
        let (key, values, continuation) = deserializer.deserialize_seq(cell_deserialize_visitor)?;
        Ok(Self {
            key,
            values,
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
        assert_eq!(decoded.continuation(), None);
    }

    #[test]
    fn test_cell_cbor_roundtrip_with_continuation() {
        let key = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63]; // 99u64.to_be_bytes()
        let values: Vec<ScalarValue> = vec![];
        let continuation = Some(42u32);
        let cell = Cell::new(key.clone(), values.clone(), continuation);

        // Serialize with CBOR
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&cell, &mut cbor).unwrap();

        // Deserialize from CBOR
        let decoded: Cell = ciborium::de::from_reader(&cbor[..]).unwrap();

        assert_eq!(decoded.key(), key.as_slice());
        assert_eq!(decoded.values(), values.as_slice());
        assert_eq!(decoded.continuation(), continuation);
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
