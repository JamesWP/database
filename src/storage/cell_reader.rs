use serde::Deserialize;

use super::cell::Key;
use super::node::NodePage;
use super::pager::Pager;

pub struct CellReader<'a> {
    pager: &'a Pager,
    key: Key,
    continuation: Option<u32>,

    // Owned buffer - safe, no dangling pointers
    buf: Vec<u8>,
    buf_pos: usize,
}

impl<'a> std::io::Read for CellReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Read from current position in buffer
        let available = self.buf.len() - self.buf_pos;
        if available > 0 {
            let to_copy = available.min(buf.len());
            buf[..to_copy].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + to_copy]);
            self.buf_pos += to_copy;
            return Ok(to_copy);
        }

        // If we've exhausted the buffer, try to load overflow page
        match self.continuation {
            None => Ok(0),
            Some(continuation) => {
                let node: Box<NodePage> = Box::new(self.pager.get_and_decode(continuation));
                let overflow_page = match node.as_ref() {
                    NodePage::OverflowPage(p) => p,
                    _ => panic!("Expected overflow page"),
                };

                // Append overflow data to buffer
                self.buf.extend_from_slice(overflow_page.value());
                self.continuation = overflow_page.continuation();

                // Try reading again
                self.read(buf)
            }
        }
    }
}

impl<'a> CellReader<'a> {
    pub fn new(pager: &'a Pager, leaf_page_idx: u32, cell_idx: usize) -> Option<CellReader<'a>> {
        let node: Box<NodePage> = Box::new(pager.get_and_decode(leaf_page_idx));

        let leaf_page = node
            .leaf()
            .expect("Values are always supposed to be in leaf pages");

        let cell = leaf_page.get_item_at_index(cell_idx)?;
        let key = cell.key();
        let continuation = cell.continuation();
        let value = cell.value();

        // Copy value bytes into owned buffer - safe, no dangling pointers
        let buf = value.to_vec();

        Some(CellReader {
            pager,
            buf,
            buf_pos: 0,
            key,
            continuation,
        })
    }

    pub fn key(&self) -> Key {
        self.key
    }

    pub fn decode_as_json_array(&mut self) -> Vec<serde_json::Value> {
        let mut deserializer = serde_json::Deserializer::from_reader(self);
        let values = Vec::<serde_json::Value>::deserialize(&mut deserializer).unwrap();
        values
    }
}

#[cfg(test)]
mod tests {
    use crate::test::TestDb;
    use std::io::Read;

    #[test]
    fn test_cell_reader_basic() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert a small value (no overflow)
        let key = 100u64;
        let value = serde_json::to_vec(&vec![1, 2, 3]).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), key);

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value);
        }
    }

    #[test]
    fn test_cell_overflow_value() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert a value larger than CHUNK_THRESHOLD (55 bytes)
        let key = 200u64;
        let large_value = vec![42u8; 100]; // 100 bytes
        let value_json = serde_json::to_vec(&large_value).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), key);

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value_json);

            // Verify the data deserializes correctly
            let decoded: Vec<u8> = serde_json::from_slice(&buf).unwrap();
            assert_eq!(decoded, large_value);
        }
    }

    #[test]
    fn test_cell_multi_page_overflow() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert a value that spans multiple overflow pages (> 155 bytes)
        let key = 300u64;
        let very_large_value = vec![99u8; 300]; // 300 bytes
        let value_json = serde_json::to_vec(&very_large_value).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), key);

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value_json);

            // Verify the data deserializes correctly
            let decoded: Vec<u8> = serde_json::from_slice(&buf).unwrap();
            assert_eq!(decoded, very_large_value);
        }
    }

    #[test]
    fn test_decode_as_json_array() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert a JSON array like [1, "alice", 30]
        let key = 400u64;
        let json_array = serde_json::json!([1, "alice", 30]);
        let value_json = serde_json::to_vec(&json_array).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_json_array();

            assert_eq!(decoded.len(), 3);
            assert_eq!(decoded[0], serde_json::json!(1));
            assert_eq!(decoded[1], serde_json::json!("alice"));
            assert_eq!(decoded[2], serde_json::json!(30));
        }
    }

    #[test]
    fn test_decode_as_json_array_types() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert a JSON array with various types
        let key = 500u64;
        let json_array = serde_json::json!([42, 3.14, "hello", true, false]);
        let value_json = serde_json::to_vec(&json_array).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_json_array();

            assert_eq!(decoded.len(), 5);
            assert_eq!(decoded[0], serde_json::json!(42));
            assert_eq!(decoded[1], serde_json::json!(3.14));
            assert_eq!(decoded[2], serde_json::json!("hello"));
            assert_eq!(decoded[3], serde_json::json!(true));
            assert_eq!(decoded[4], serde_json::json!(false));
        }
    }

    #[test]
    fn test_cell_empty_value() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        // Insert an empty array
        let key = 600u64;
        let json_array = serde_json::json!([]);
        let value_json = serde_json::to_vec(&json_array).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_json_array();

            assert_eq!(decoded.len(), 0);
        }
    }
}
