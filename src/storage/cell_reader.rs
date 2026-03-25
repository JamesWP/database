use crate::engine::scalarvalue::ScalarValue;
use probe::probe;

use super::node::NodePage;
use super::pager::Pager;

pub struct CellReader<'a> {
    pager: &'a Pager,
    key: Vec<u8>,
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
                probe!(database, page_read_overflow);
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
        probe!(database, page_read_leaf);

        let leaf_page = node
            .leaf()
            .expect("Values are always supposed to be in leaf pages");

        let cell = leaf_page.get_item_at_index(cell_idx)?;
        let key = cell.key().to_vec();
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

    /// Returns the raw key bytes for this cell.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn decode_as_array(&mut self) -> Vec<ScalarValue> {
        probe!(database, cbor_row_decode);
        ciborium::de::from_reader(self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::scalarvalue::ScalarValue;
    use crate::storage::BTree;
    use crate::test::TestDb;
    use std::io::Read;

    #[test]
    fn test_cell_reader_basic() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert a small value (no overflow)
        let key = 100u64;
        let mut value = Vec::new();
        ciborium::ser::into_writer(&vec![1, 2, 3], &mut value).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value);
        }
    }

    #[test]
    fn test_cell_overflow_value() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert a value larger than CHUNK_THRESHOLD (55 bytes)
        let key = 200u64;
        let large_value = vec![42u8; 100]; // 100 bytes
        let mut value_json = Vec::new();
        ciborium::ser::into_writer(&large_value, &mut value_json).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value_json);

            // Verify the data deserializes correctly
            let decoded: Vec<u8> = ciborium::de::from_reader(&buf[..]).unwrap();
            assert_eq!(decoded, large_value);
        }
    }

    #[test]
    fn test_cell_multi_page_overflow() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert a value that spans multiple overflow pages (> 155 bytes)
        let key = 300u64;
        let very_large_value = vec![99u8; 300]; // 300 bytes
        let mut value_json = Vec::new();
        ciborium::ser::into_writer(&very_large_value, &mut value_json).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value_json.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, value_json);

            // Verify the data deserializes correctly
            let decoded: Vec<u8> = ciborium::de::from_reader(&buf[..]).unwrap();
            assert_eq!(decoded, very_large_value);
        }
    }

    #[test]
    fn test_decode_as_array() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert an array like [1, "alice", 30]
        let key = 400u64;
        let values = vec![
            ScalarValue::Integer(1),
            ScalarValue::String("alice".to_string()),
            ScalarValue::Integer(30),
        ];
        let mut value_bytes = Vec::new();
        ciborium::ser::into_writer(&values, &mut value_bytes).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value_bytes.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_array();

            assert_eq!(decoded.len(), 3);
            assert_eq!(decoded[0], ScalarValue::Integer(1));
            assert_eq!(decoded[1], ScalarValue::String("alice".to_string()));
            assert_eq!(decoded[2], ScalarValue::Integer(30));
        }
    }

    #[test]
    fn test_decode_as_array_types() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert an array with various types
        let key = 500u64;
        let values = vec![
            ScalarValue::Integer(42),
            ScalarValue::Floating(3.14),
            ScalarValue::String("hello".to_string()),
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
        ];
        let mut value_bytes = Vec::new();
        ciborium::ser::into_writer(&values, &mut value_bytes).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value_bytes.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_array();

            assert_eq!(decoded.len(), 5);
            assert_eq!(decoded[0], ScalarValue::Integer(42));
            assert_eq!(decoded[1], ScalarValue::Floating(3.14));
            assert_eq!(decoded[2], ScalarValue::String("hello".to_string()));
            assert_eq!(decoded[3], ScalarValue::Boolean(true));
            assert_eq!(decoded[4], ScalarValue::Boolean(false));
        }
    }

    #[test]
    fn test_cell_empty_value() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root_page = btree.create_tree();

        // Insert an empty array
        let key = 600u64;
        let values: Vec<ScalarValue> = vec![];
        let mut value_bytes = Vec::new();
        ciborium::ser::into_writer(&values, &mut value_bytes).unwrap();
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value_bytes.clone());
        }

        // Read it back with CellReader
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_array();

            assert_eq!(decoded.len(), 0);
        }
    }
}
