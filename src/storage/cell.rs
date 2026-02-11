use serde::{de::Visitor, Deserialize, Serialize};

pub type Key = u64;
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

    pub fn key(&self) -> Key {
        self.key
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
    type Value = (u64, Vec<u8>, Option<u32>);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of two or three values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let key = seq.next_element()?.unwrap();
        let value = seq.next_element()?.unwrap();
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
    fn test_cell_roundtrip() {
        let key = 12345u64;
        let value = b"test value".to_vec();
        let cell = Cell::new(key, value.clone(), None);

        // Serialize
        let json = serde_json::to_vec(&cell).unwrap();

        // Deserialize
        let decoded: Cell = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.key(), key);
        assert_eq!(decoded.value(), value.as_slice());
        assert_eq!(decoded.continuation(), None);
    }

    #[test]
    fn test_cell_roundtrip_with_continuation() {
        let key = 99u64;
        let value = b"overflow data".to_vec();
        let continuation = Some(42u32);
        let cell = Cell::new(key, value.clone(), continuation);

        // Serialize
        let json = serde_json::to_vec(&cell).unwrap();

        // Deserialize
        let decoded: Cell = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.key(), key);
        assert_eq!(decoded.value(), value.as_slice());
        assert_eq!(decoded.continuation(), continuation);
    }

    #[test]
    fn test_cell_empty_value() {
        let key = 1u64;
        let value = Vec::new();
        let cell = Cell::new(key, value.clone(), None);

        // Serialize
        let json = serde_json::to_vec(&cell).unwrap();

        // Deserialize
        let decoded: Cell = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.key(), key);
        assert_eq!(decoded.value(), &[] as &[u8]);
        assert_eq!(decoded.continuation(), None);
    }
}
