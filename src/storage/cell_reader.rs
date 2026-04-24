use crate::engine::scalarvalue::ScalarValue;
use probe::probe;

use super::node::NodePage;
use super::node_page_store::NodePageStore;
use super::page_id::PageId;

/// Reads cell data from a leaf page, following overflow chains eagerly on
/// construction.  After `new` returns the store borrow is released — no
/// lifetime tie to the store.
pub enum CellReader {
    /// Cell is stored inline — values are available directly with no decode.
    Inline {
        key: Vec<u8>,
        values: Vec<ScalarValue>,
    },
    /// Cell has an overflow chain — bytes are assembled eagerly; decode on
    /// `decode_as_array`.
    Overflow {
        key: Vec<u8>,
        buf: Vec<u8>,
        buf_pos: usize,
    },
}

impl std::io::Read for CellReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            CellReader::Inline { .. } => Ok(0),
            CellReader::Overflow {
                buf: data, buf_pos, ..
            } => {
                let available = data.len() - *buf_pos;
                if available == 0 {
                    return Ok(0);
                }
                let n = available.min(buf.len());
                buf[..n].copy_from_slice(&data[*buf_pos..*buf_pos + n]);
                *buf_pos += n;
                Ok(n)
            }
        }
    }
}

impl CellReader {
    /// Construct a `CellReader` for the cell at `cell_idx` on `leaf_page_idx`.
    ///
    /// For inline cells the values are cloned from the page and no further I/O
    /// is needed.  For overflow cells all overflow pages are followed eagerly so
    /// the store borrow does not outlive this function.  Returns `None` if the
    /// cell does not exist.
    pub fn new(
        store: &mut NodePageStore,
        leaf_page_idx: u32,
        cell_idx: usize,
    ) -> Option<CellReader> {
        let (key, values, inline_bytes, continuation) = {
            let node = store.read(PageId(leaf_page_idx)).ok()?;
            let leaf = node.leaf()?;
            let cell = leaf.get_item_at_index(cell_idx)?;
            (
                cell.key().to_vec(),
                cell.values().to_vec(),
                cell.inline_bytes().to_vec(),
                cell.continuation(),
            )
        }; // node borrow ends here

        if continuation.is_none() {
            return Some(CellReader::Inline { key, values });
        }

        // Overflow path: assemble CBOR bytes starting with the inline prefix,
        // then following the overflow chain.
        let mut buf = inline_bytes;
        let mut next = continuation;
        while let Some(cont_page) = next {
            probe!(database, overflow_read, cont_page);
            let (content, continuation_next) = {
                let node = store.read(PageId(cont_page)).ok()?;
                let overflow = match node {
                    NodePage::OverflowPage(p) => p,
                    _ => return None,
                };
                (overflow.value().to_vec(), overflow.continuation())
            }; // node borrow ends here
            buf.extend_from_slice(&content);
            next = continuation_next;
        }

        Some(CellReader::Overflow {
            key,
            buf,
            buf_pos: 0,
        })
    }

    /// Returns the raw key bytes for this cell.
    pub fn key(&self) -> &[u8] {
        match self {
            CellReader::Inline { key, .. } => key,
            CellReader::Overflow { key, .. } => key,
        }
    }

    pub fn decode_as_array(&mut self) -> Vec<ScalarValue> {
        match self {
            CellReader::Inline { values, .. } => {
                probe!(database, cell_read_inline);
                values.clone()
            }
            CellReader::Overflow { .. } => {
                probe!(database, cbor_overflow_row_decode);
                ciborium::de::from_reader(self).unwrap()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::scalarvalue::ScalarValue;
    use crate::test::TestDb;
    use std::io::Read;

    #[test]
    fn test_cell_reader_basic() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 100u64;
        let values = vec![
            ScalarValue::Integer(1),
            ScalarValue::Integer(2),
            ScalarValue::Integer(3),
        ];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let decoded = reader.decode_as_array();
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn test_cell_overflow_value() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 200u64;
        // A string long enough to exceed CHUNK_THRESHOLD when CBOR-encoded
        let large_string = "x".repeat(1100);
        let values = vec![ScalarValue::String(large_string.clone())];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let decoded = reader.decode_as_array();
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn test_cell_multi_page_overflow() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 300u64;
        // Large enough to span multiple overflow pages (> OVERFLOW_LIMIT bytes of CBOR)
        let large_string = "y".repeat(5000);
        let values = vec![ScalarValue::String(large_string.clone())];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            assert_eq!(reader.key(), crate::storage::encode_u64_key(key).as_slice());

            let decoded = reader.decode_as_array();
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn test_decode_as_array() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 400u64;
        let values = vec![
            ScalarValue::Integer(1),
            ScalarValue::String("alice".to_string()),
            ScalarValue::Integer(30),
        ];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
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
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 500u64;
        let values = vec![
            ScalarValue::Integer(42),
            ScalarValue::Floating(3.14),
            ScalarValue::String("hello".to_string()),
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
        ];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
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
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        let key = 600u64;
        let values: Vec<ScalarValue> = vec![];
        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, values.clone());
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let decoded = reader.decode_as_array();

            assert_eq!(decoded.len(), 0);
        }
    }

    /// Verify that a CellReader for an inline cell returns Ok(0) from io::Read —
    /// the io::Read impl is only meaningful for overflow cells.
    #[test]
    fn test_inline_cell_read_returns_eof() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root_page = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::Integer(99)]);
        }

        {
            let mut cursor_handle = btree.open(root_page);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut reader = cursor.get_entry().unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert!(buf.is_empty(), "inline cells return no bytes via io::Read");
        }
    }
}
