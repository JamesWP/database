use serde::{de::Visitor, Deserialize, Serialize};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type ValueRef<'a> = &'a [u8];

#[derive(Debug, Clone)]
pub struct Cell {
    key: Key,
    value: Value,
    continuation: Option<u32>,
}

impl Cell {
    pub fn new(key: Key, value: Value, continuation: Option<u32>) -> Cell {
        Cell {
            key,
            value,
            continuation,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> ValueRef<'_> {
        &self.value
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
        match self.continuation {
            Some(continuation) => (&self.key, &self.value, continuation).serialize(serializer),
            None => (&self.key, &self.value).serialize(serializer),
        }
    }
}

struct CellDeserializeVisitor;
impl<'de> Visitor<'de> for CellDeserializeVisitor {
    type Value = (Vec<u8>, Vec<u8>, Option<u32>);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of two or three values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let key: Vec<u8> = seq.next_element()?.unwrap();
        let value: Vec<u8> = seq.next_element()?.unwrap();
        let continuation = seq.next_element()?;

        Ok((key, value, continuation))
    }
}

impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let cell_deserialize_visitor = CellDeserializeVisitor {};
        let (key, value, continuation) = deserializer.deserialize_seq(cell_deserialize_visitor)?;
        Ok(Self {
            key,
            value,
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
        let value = b"test value".to_vec();
        let cell = Cell::new(key.clone(), value.clone(), None);

        // Serialize with CBOR
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&cell, &mut cbor).unwrap();

        // Deserialize from CBOR
        let decoded: Cell = ciborium::de::from_reader(&cbor[..]).unwrap();

        assert_eq!(decoded.key(), key.as_slice());
        assert_eq!(decoded.value(), value.as_slice());
        assert_eq!(decoded.continuation(), None);
    }

    #[test]
    fn test_cell_cbor_roundtrip_with_continuation() {
        let key = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63]; // 99u64.to_be_bytes()
        let value = b"overflow data".to_vec();
        let continuation = Some(42u32);
        let cell = Cell::new(key.clone(), value.clone(), continuation);

        // Serialize with CBOR
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&cell, &mut cbor).unwrap();

        // Deserialize from CBOR
        let decoded: Cell = ciborium::de::from_reader(&cbor[..]).unwrap();

        assert_eq!(decoded.key(), key.as_slice());
        assert_eq!(decoded.value(), value.as_slice());
        assert_eq!(decoded.continuation(), continuation);
    }

    #[test]
    fn test_cell_variable_length_keys() {
        // Short key
        let short_key = vec![0x01];
        let cell = Cell::new(short_key.clone(), b"v".to_vec(), None);
        assert_eq!(cell.key(), short_key.as_slice());

        // Long key (1KB)
        let long_key = vec![0xABu8; 1024];
        let cell = Cell::new(long_key.clone(), b"v".to_vec(), None);
        assert_eq!(cell.key(), long_key.as_slice());

        // Empty key
        let empty_key = vec![];
        let cell = Cell::new(empty_key.clone(), b"v".to_vec(), None);
        assert_eq!(cell.key(), empty_key.as_slice());
    }
}
