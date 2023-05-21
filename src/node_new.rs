use std::io::Read;

use serde::{Deserialize, Serialize, de::Visitor};

use crate::node;

#[derive(Debug)]
struct Cell {
    key: u64,
    value: String,
    continuation: Option<u32>,
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
    type Value = (u64, String, Option<u32>);

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

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    cells: Vec<Cell>,
}

struct OverflowPage {
    cell: String,
    continuation: Option<u32>,
}

impl Node {
    fn get(&self, i: usize) -> Option<&Cell> {
        let cell = self.cells.get(i)?;
        Some(cell)
    }
}

enum PageType<'l> {
    Node(&'l Node),
    Overflow(&'l OverflowPage),
}

struct Pager {
    cell_page: Node,
    overflow_one: OverflowPage,
    overflow_two: OverflowPage,
}

impl Pager {
    fn new() -> Pager {
        Pager {
            cell_page: Node {
                cells: vec![
                    Cell {
                        key: 1,
                        value: "1234".to_owned(),
                        continuation: None,
                    },
                    Cell {
                        key: 1,
                        value: "[1,2,3".to_owned(),
                        continuation: Some(2),
                    },
                ],
            },
            overflow_one: OverflowPage {
                cell: ",4,5,6,".to_owned(),
                continuation: Some(3),
            },
            overflow_two: OverflowPage {
                cell: "7,8,9]".to_owned(),
                continuation: None,
            },
        }
    }

    fn get(&self, page_no: u32) -> Option<PageType> {
        match page_no {
            1 => Some(PageType::Node(&self.cell_page)),
            2 => Some(PageType::Overflow(&self.overflow_one)),
            3 => Some(PageType::Overflow(&self.overflow_two)),
            _ => None,
        }
    }
}

struct CellReader<'pager> {
    pager: &'pager Pager,
    curent_buf: &'pager [u8],
    next_continuation: Option<u32>,
}

impl<'cell, 'pager, 'overflow> std::io::Read for CellReader<'pager> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.curent_buf.read(buf)?;

        if bytes_read != 0 {
            return Ok(bytes_read);
        }

        match self.next_continuation {
            None => Ok(0),
            Some(continuation) => {
                let next_page = self.pager.get(continuation).unwrap();
                let overflow_page = match next_page {
                    PageType::Overflow(o) => o,
                    PageType::Node(_) => todo!(),
                };
                self.curent_buf = overflow_page.cell.as_bytes();
                self.next_continuation = overflow_page.continuation;
                self.curent_buf.read(buf)
            }
        }
    }
}

#[cfg(test)]
type Tuple = Vec<serde_json::Value>;

#[test]
fn test_reading() {
    let p = Pager::new();

    let node_page = p.get(1).unwrap();
    let node = match node_page {
        PageType::Node(ref n) => n,
        PageType::Overflow(_) => todo!(),
    };
    let cell_ref = node.get(1).unwrap();
    let mut cell_reader = CellReader {
        pager: &p,
        curent_buf: cell_ref.value.as_bytes(),
        next_continuation: cell_ref.continuation
    };

    // let mut buf = vec![];
    // let len = cell_reader.read_to_end(&mut buf).unwrap();

    // assert_eq!(len, 19);

    let mut deserializer = serde_json::Deserializer::from_reader(&mut cell_reader);
    let value =Tuple::deserialize(&mut deserializer).unwrap();

    assert_eq!(value.len(), 9);
}

#[test]
fn test_encode() {
    let p = Pager::new();
    let node_page = p.get(1).unwrap();
    let node = match node_page {
        PageType::Node(n) => n,
        PageType::Overflow(_) => todo!(),
    };
    let str = serde_json::to_string(node).unwrap();
    println!("{}", str);
    assert_eq!(&str, "wat");
}

#[test]
fn test_decode() {
    let input = "{\"cells\":[[1,\"1234\"],[1,\"[1,2,3\",2]]}";
    let node_page: Node = serde_json::from_slice(input.as_bytes()).unwrap();

    println!("node {:?}", node_page);
}