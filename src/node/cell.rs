use serde::{Serialize, de::Visitor, Deserialize};

pub type Key=u64;
pub type Value=Vec<u8>;
pub type ValueRef<'a> = &'a [u8];

#[derive(Debug, Clone)]
pub struct Cell {
    key: Key,
    value: Value,
    continuation: Option<u32>,
}

impl Cell {
    pub fn new(key: Key, value: Value, continuation: Option<u32>) -> Cell {
        Cell {key, value, continuation}
    }  

    pub fn key(&self) -> Key {
        self.key
    }

    pub fn value(&self) -> ValueRef {
        &self.value
    }
}

impl Serialize for Cell {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        match self.continuation {
            Some(continuation) => (&self.key, &self.value, continuation).serialize(serializer),
            None => (&self.key, &self.value).serialize(serializer)
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
            A: serde::de::SeqAccess<'de>, {
        let key = seq.next_element()?.unwrap();
        let value = seq.next_element()?.unwrap();
        let continuation = seq.next_element()?;

        Ok((key, value, continuation))
    }
}

impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let cell_deserialize_visitor = CellDeserializeVisitor{};
        let (key, value, continuation) = deserializer.deserialize_seq(cell_deserialize_visitor)?;
        Ok(Self {key, value, continuation})
    }
}