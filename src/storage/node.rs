use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

use crate::engine::scalarvalue::ScalarValue;

use super::cell::{Cell, Key};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum NodePage {
    Leaf(LeafNodePage),
    Interior(InteriorNodePage),
    OverflowPage(OverflowPage),
}

impl NodePage {
    /// Conservative upper-bound estimate of the CBOR-encoded size of this page in bytes.
    ///
    /// Used in `NodePageStore::write` to decide whether to encode for a `PageFull` check
    /// without paying for a full serialisation on every write.  Must never underestimate —
    /// false positives (estimate > PAGE_SIZE but actual ≤ PAGE_SIZE) are safe; false
    /// negatives would silently produce an oversized page at flush time.
    ///
    /// `OverflowPage` returns 0 because `split_and_store` always chunks data so each
    /// overflow page fits by construction.
    pub fn cbor_size_estimate(&self) -> usize {
        match self {
            NodePage::Leaf(l) => l.cbor_size_estimate(),
            NodePage::Interior(i) => i.cbor_size_estimate(),
            NodePage::OverflowPage(_) => 0,
        }
    }

    pub fn search(&self, k: &[u8]) -> SearchResult {
        match self {
            NodePage::Leaf(l) => l.search(k),
            NodePage::Interior(i) => i.search(k),
            _ => panic!(),
        }
    }

    // TODO: inserting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn insert_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.insert_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!(),
        };
    }

    // TODO: setting an item into an interior page doesn't make sense, interior pages dont store values!
    pub fn set_item_at_index(&mut self, item_idx: usize, cell: Cell) {
        match self {
            NodePage::Leaf(l) => {
                l.set_item_at_index(item_idx, cell);
            }
            NodePage::Interior(_) => todo!(),
            _ => panic!(),
        };
    }

    pub fn split(self) -> (Self, Self) {
        match self {
            NodePage::Leaf(l) => {
                let (left, right) = l.split();
                (Self::Leaf(left), Self::Leaf(right))
            }
            NodePage::Interior(i) => {
                let (left, right) = i.split();
                (Self::Interior(left), Self::Interior(right))
            }
            _ => panic!(),
        }
    }

    pub fn smallest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.first().unwrap().key().to_vec(),
            NodePage::Interior(i) => i.keys.first().unwrap().clone(),
            _ => panic!(),
        }
    }

    pub fn largest_key(&self) -> Key {
        match self {
            NodePage::Leaf(l) => l.cells.last().unwrap().key().to_vec(),
            NodePage::Interior(i) => i.keys.last().unwrap().clone(),
            _ => panic!(),
        }
    }

    pub fn interior(self) -> Option<InteriorNodePage> {
        match self {
            NodePage::Interior(i) => Some(i),
            _ => None,
        }
    }

    pub fn leaf(&self) -> Option<&LeafNodePage> {
        match self {
            NodePage::Leaf(l) => Some(l),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LeafNodePage {
    cells: Vec<Cell>,
}

impl Default for LeafNodePage {
    fn default() -> Self {
        Self {
            cells: Default::default(),
        }
    }
}

pub enum SearchResult {
    /// The value was found at the given index of the given leaf node
    Found(usize),
    /// The value is not present in the leaf node, but if it were it should be at this index
    NotPresent(usize),
    /// The element wasn't found, but if it is anywhere
    /// then it must be in the child node identified by the given index and page number
    GoDown(usize, u32),
}

impl LeafNodePage {
    pub fn search(&self, search_key: &[u8]) -> SearchResult {
        // Simple linear search through the page.
        for (index, cell) in self.cells.iter().enumerate() {
            let cell_key = cell.key();
            match search_key.cmp(cell_key) {
                Less => return SearchResult::NotPresent(index),
                Equal => return SearchResult::Found(index),
                Greater => {} // Continue the search
            }
        }

        SearchResult::NotPresent(self.cells.len())
    }

    pub fn set_item_at_index(&mut self, index: usize, cell: Cell) {
        self.cells[index] = cell;
    }

    pub fn insert_item_at_index(&mut self, index: usize, cell: Cell) {
        self.cells.insert(index, cell);
    }

    pub fn remove_cell(&mut self, index: usize) {
        if index < self.cells.len() {
            self.cells.remove(index);
        }
    }

    pub fn get_item_at_index<'a>(&'a self, entry_index: usize) -> Option<&'a Cell> {
        self.cells.get(entry_index)
    }

    pub fn num_items(&self) -> usize {
        self.cells.len()
    }

    pub fn get_key(&self, index: usize) -> Key {
        self.cells[index].key().to_vec()
    }

    pub fn verify_key_ordering(&self) -> Result<(), VerifyError> {
        for window in self.cells.windows(2) {
            if window[0].key() > window[1].key() {
                return Err(VerifyError::KeyOutOfOrder);
            }
        }
        Ok(())
    }

    fn cbor_size_estimate(&self) -> usize {
        // 15 = LEAF_PAGE_BASE_FRAMING_BYTES (measured for an empty leaf).
        // +2 = slack for the `cells` CBOR array header growing from 1 byte (≤ 23 elements)
        //      to 3 bytes (≥ 256 elements) as more cells are added.  Index leaf pages can
        //      hold hundreds of small cells, so this growth is reachable.
        let mut n: usize = 17;
        for cell in &self.cells {
            n += cell_cbor_size_estimate(cell);
        }
        n
    }

    fn split(&self) -> (LeafNodePage, LeafNodePage) {
        //TODO: can this take self by value?

        let midpoint = self.cells.len() / 2;
        let (left, right) = self.cells.split_at(midpoint);

        let left = Self {
            cells: left.to_vec(),
        };

        let right = Self {
            cells: right.to_vec(),
        };

        (left, right)
    }
}

#[derive(Debug)]
pub enum VerifyError {
    KeyOutOfOrder,
}

/// Serde module for `Vec<Vec<u8>>`: serializes each inner `Vec<u8>` as a CBOR byte string
/// (major type 2) rather than a CBOR array of integers, matching the `serde_bytes` approach
/// used for individual byte slices.
mod serde_vec_bytes {
    use serde::{
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
        Deserializer, Serializer,
    };

    pub fn serialize<S>(keys: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(keys.len()))?;
        for key in keys {
            seq.serialize_element(serde_bytes::Bytes::new(key))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ByteVecVisitor;
        impl<'de> Visitor<'de> for ByteVecVisitor {
            type Value = Vec<Vec<u8>>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("sequence of byte strings")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(buf) = seq.next_element::<serde_bytes::ByteBuf>()? {
                    out.push(buf.into_vec());
                }
                Ok(out)
            }
        }
        deserializer.deserialize_seq(ByteVecVisitor)
    }
}

// [edge 0] [key 0] [edge 1] [key 1] ... [key N-1] [edge N]
// items in [edge i] are LESS than or EQUAL to [key i]
// (if there is no [key i], i.e. at the end, items in [edge i] must be GREATER than [key i-1])
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InteriorNodePage {
    #[serde(with = "serde_vec_bytes")]
    keys: Vec<Key>,
    edges: Vec<u32>,
}

impl InteriorNodePage {
    pub fn new(
        left_page_idx: u32,
        right_page_smallest_key: Key,
        right_page_idx: u32,
    ) -> InteriorNodePage {
        InteriorNodePage {
            keys: vec![right_page_smallest_key],
            edges: vec![left_page_idx, right_page_idx],
        }
    }

    pub fn get_child_page_by_index(&self, arg: usize) -> u32 {
        self.edges[arg].clone()
    }

    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }

    pub fn verify_key_ordering(&self) -> Result<(), VerifyError> {
        for window in self.keys.windows(2) {
            if window[0] > window[1] {
                return Err(VerifyError::KeyOutOfOrder);
            }
        }
        Ok(())
    }

    pub fn get_key_by_index(&self, edge: usize) -> Key {
        self.keys[edge].clone()
    }

    fn search(&self, k: &[u8]) -> SearchResult {
        for (idx, key) in self.keys.iter().enumerate() {
            match k.cmp(key.as_slice()) {
                Less => {
                    return SearchResult::GoDown(idx, self.edges[idx]);
                }
                Equal => return SearchResult::GoDown(idx + 1, self.edges[idx + 1]),
                Greater => {
                    continue;
                }
            };
        }

        SearchResult::GoDown(self.edges.len() - 1, self.edges.last().unwrap().clone())
    }

    pub fn node(self) -> NodePage {
        NodePage::Interior(self)
    }

    pub fn insert_child_page(&mut self, edge_page_smallest_key: Key, edge_page_idx: u32) {
        for (idx, key) in self.keys.iter().enumerate() {
            match edge_page_smallest_key.as_slice().cmp(key.as_slice()) {
                Less => {
                    self.edges.insert(idx + 1, edge_page_idx);
                    self.keys.insert(idx, edge_page_smallest_key);
                    return;
                }
                Equal => panic!("Don't think this is possible"),
                Greater => {
                    continue;
                }
            }
        }

        self.edges.push(edge_page_idx);
        self.keys.push(edge_page_smallest_key);
    }

    fn cbor_size_estimate(&self) -> usize {
        // Conservative base for an empty interior page.
        // +4 = slack for two CBOR array headers (`keys` and `edges`), each able to grow
        //      from 1 byte (≤ 23 elements) to 3 bytes (≥ 256 elements) = +2 each.
        let mut n: usize = 24;
        for key in &self.keys {
            // CBOR byte-string: 1-3 byte length header + key data.
            let hdr = if key.len() <= 23 { 1 } else if key.len() <= 0xFF { 2 } else { 3 };
            n += hdr + key.len();
        }
        // Each child page number is a u32: 1–5 CBOR bytes; assume worst-case 5.
        n += 5 * self.edges.len();
        n
    }

    fn split(&self) -> (InteriorNodePage, InteriorNodePage) {
        /*
            W  E  R
          [A][S][D][F]

          left:     right:
            W          R
          [A][S]     [D][F]

          E is no longer required
        */

        // invariant each of the two interior pages produced must have at least two child pages and one key
        assert!(self.keys.len() >= 3); // One key is removed in the split
        assert!(self.edges.len() >= 4);

        let (left_keys, right_keys) = self.keys.split_at(self.keys.len() / 2);

        // we must take the extra key in the right side and remove it.
        let right_keys = &right_keys[1..];

        let (left_edges, right_edges) = self.edges.split_at((self.edges.len() + 1) / 2);

        assert_eq!(left_keys.len() + 1, left_edges.len());
        assert_eq!(right_keys.len() + 1, right_edges.len());

        let left = Self {
            edges: left_edges.to_vec(),
            keys: left_keys.to_vec(),
        };
        let right = Self {
            edges: right_edges.to_vec(),
            keys: right_keys.to_vec(),
        };
        (left, right)
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OverflowPage {
    #[serde(with = "serde_bytes")]
    content: Vec<u8>,
    continuation: Option<u32>,
}

impl OverflowPage {
    pub fn new(content: Vec<u8>, continuation: Option<u32>) -> OverflowPage {
        OverflowPage {
            content,
            continuation,
        }
    }

    pub fn continuation(&self) -> Option<u32> {
        self.continuation
    }

    pub fn value(&self) -> &[u8] {
        &self.content
    }
}

// ── CBOR size estimation helpers ──────────────────────────────────────────────

/// Conservative upper-bound estimate of the CBOR size of one `Cell`.
///
/// Mirrors the logic in `btree.rs::cbor_size_estimate` (for the values portion)
/// plus the per-cell framing constants.  Must never underestimate.
fn cell_cbor_size_estimate(cell: &Cell) -> usize {
    let key_len = cell.key().len();
    // CBOR byte-string header: 1 byte for ≤ 23, 2 for ≤ 255, 3 for ≤ 65535, 5 otherwise.
    let key_hdr = if key_len <= 23 { 1 } else if key_len <= 0xFF { 2 } else if key_len <= 0xFFFF { 3 } else { 5 };
    if cell.continuation().is_some() {
        // Overflow cell: [key_bytes, inline_bytes, u32_cont]
        //   1 (outer array) + key_hdr + key_len
        //   + 3 (bytes hdr, inline_bytes > 255) + inline_bytes.len()
        //   + 5 (u32, worst-case 5 bytes)
        1 + key_hdr + key_len + 3 + cell.inline_bytes().len() + 5
    } else {
        // Inline cell: [key_bytes, values_array]
        //   1 (outer array) + key_hdr + key_len + values
        1 + key_hdr + key_len + scalar_values_cbor_size_estimate(cell.values())
    }
}

/// Conservative upper-bound estimate of the CBOR size of `Vec<ScalarValue>`.
///
/// ScalarValue uses serde's externally-tagged enum representation:
///   unit variant `Null`  → text "Null" (5 bytes)
///   newtype variants     → `{variant_name: value}` CBOR map
/// Kept in sync with `cbor_size_estimate` in `btree.rs`.
fn scalar_values_cbor_size_estimate(values: &[ScalarValue]) -> usize {
    let mut n: usize = 5; // conservative array header
    for v in values {
        n += match v {
            ScalarValue::Null => 5,
            ScalarValue::Boolean(_) => 10,
            ScalarValue::Integer(i) => {
                let abs = i.unsigned_abs();
                let val = if abs <= 23 { 1 } else if abs <= 0xFF { 2 }
                          else if abs <= 0xFFFF { 3 } else if abs <= 0xFFFF_FFFF { 5 } else { 9 };
                9 + val
            }
            ScalarValue::Floating(_) => 19,
            ScalarValue::String(s) => {
                let len = s.len();
                let hdr = if len <= 23 { 1 } else if len <= 0xFF { 2 }
                          else if len <= 0xFFFF { 3 } else { 5 };
                8 + hdr + len
            }
            ScalarValue::Blob(b) => {
                // Blob uses #[serde(with = "serde_bytes")] → CBOR byte string.
                let len = b.len();
                let hdr = if len <= 23 { 1 } else if len <= 0xFF { 2 }
                          else if len <= 0xFFFF { 3 } else { 5 };
                6 + hdr + len
            }
        };
    }
    n
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{Cell, NodePage, OverflowPage};

    use super::{InteriorNodePage, LeafNodePage, SearchResult};
    use crate::engine::scalarvalue::ScalarValue;

    fn make_key(k: u64) -> Vec<u8> {
        k.to_be_bytes().to_vec()
    }

    #[test]
    fn test_insertion_ordering() {
        let mut page = LeafNodePage::default();

        // []
        page.insert_item_at_index(
            0,
            Cell::new(make_key(2), vec![ScalarValue::Integer(0)], None),
        );
        // [2]
        page.insert_item_at_index(
            0,
            Cell::new(make_key(1), vec![ScalarValue::Integer(0)], None),
        );
        // [1, 2]
        page.insert_item_at_index(
            2,
            Cell::new(make_key(3), vec![ScalarValue::Integer(0)], None),
        );
        // [1, 2, 3]

        assert_eq!(page.cells[0].key(), make_key(1).as_slice());
        assert_eq!(page.cells[1].key(), make_key(2).as_slice());
        assert_eq!(page.cells[2].key(), make_key(3).as_slice());
    }

    fn found_index(r: SearchResult) -> usize {
        match r {
            super::SearchResult::Found(i) => i,
            super::SearchResult::NotPresent(_) => panic!(),
            super::SearchResult::GoDown(_, _) => panic!(),
        }
    }

    #[test]
    fn test_search() {
        let mut page = LeafNodePage::default();

        page.insert_item_at_index(
            0,
            Cell::new(make_key(1), vec![ScalarValue::Integer(0)], None),
        );
        page.insert_item_at_index(
            1,
            Cell::new(make_key(2), vec![ScalarValue::Integer(0)], None),
        );
        page.insert_item_at_index(
            2,
            Cell::new(make_key(3), vec![ScalarValue::Integer(0)], None),
        );

        println!("Page: {:?}", page);
        assert_eq!(0, found_index(page.search(&make_key(1))));
        assert_eq!(1, found_index(page.search(&make_key(2))));
        assert_eq!(2, found_index(page.search(&make_key(3))));
    }

    #[test]
    fn test_lexicographic_ordering() {
        // Verify that byte-slice comparison maintains lexicographic order.
        // Keys "a" < "ab" < "b" < "ba"
        let mut page = LeafNodePage::default();

        let keys: Vec<Vec<u8>> = vec![b"a".to_vec(), b"ab".to_vec(), b"b".to_vec(), b"ba".to_vec()];

        for (idx, key) in keys.iter().enumerate() {
            let result = page.search(key);
            match result {
                SearchResult::NotPresent(i) => page.insert_item_at_index(
                    i,
                    Cell::new(key.clone(), vec![ScalarValue::Integer(idx as i64)], None),
                ),
                SearchResult::Found(_) => panic!("Unexpected duplicate"),
                SearchResult::GoDown(_, _) => panic!(),
            }
        }

        page.verify_key_ordering().unwrap();
        assert_eq!(page.num_items(), 4);
        assert_eq!(page.cells[0].key(), b"a");
        assert_eq!(page.cells[1].key(), b"ab");
        assert_eq!(page.cells[2].key(), b"b");
        assert_eq!(page.cells[3].key(), b"ba");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_split(insertions in prop::collection::vec((0..100u64, 0..1000u64),0..100usize)) {
            let mut page = LeafNodePage::default();

            // Count num unique keys
            let n = insertions.iter().map(|(k,_)| k).collect::<HashSet<_>>().len();

            for (key, value) in insertions {
                let key_bytes = key.to_be_bytes().to_vec();
                let cell = Cell::new(
                    key_bytes.clone(),
                    vec![ScalarValue::Integer(value as i64)],
                    None,
                );
                let result = page.search(&key_bytes);
                match result {
                    SearchResult::Found(idx) => page.set_item_at_index(idx, cell),
                    SearchResult::NotPresent(idx) => page.insert_item_at_index(idx, cell),
                    SearchResult::GoDown(_, _) => panic!(),
                };

                page.verify_key_ordering().unwrap();
            }

            // Page has N elements, one for each unique key
            assert_eq!(n, page.num_items());

            let (left, right) = page.split();

            // No items were lost in the making of these parts
            assert_eq!(left.num_items() + right.num_items(), n);

            let delta = left.num_items().abs_diff(right.num_items());

            assert!(delta == 0 || delta == 1);

            // If we have items in both parts, they should be in order
            if left.num_items()>0 && right.num_items()>0 {
                assert!(left.get_item_at_index(left.num_items()-1).unwrap().key() < right.get_item_at_index(0).unwrap().key());
            }
        }
    }

    #[test]
    fn test_interior_split() {
        /*
            W  E  R
          [A][S][D][F]

          left:     right:
            W          R
          [A][S]     [D][F]

          E is no longer required
        */
        let (w, e, r) = (make_key(1), make_key(2), make_key(3));
        let (a, s, d, f) = (10u32, 20u32, 30u32, 40u32);

        let mut interior_node = InteriorNodePage::new(a, w.clone(), s);
        interior_node.insert_child_page(e.clone(), d);
        interior_node.insert_child_page(r.clone(), f);

        assert_eq!(interior_node.edges, &[a, s, d, f]);
        assert_eq!(interior_node.keys, &[w, e, r]);

        let (left, right) = interior_node.split();

        assert_eq!(left.edges, &[a, s]);
        assert_eq!(left.keys, &[make_key(1)]);

        assert_eq!(right.edges, &[d, f]);
        assert_eq!(right.keys, &[make_key(3)]);
    }

    #[test]
    fn test_interior_split_balance() {
        // Verify that after a split, both sides are within 1 key of each other.
        for n_keys in 3..=20 {
            let mut node = InteriorNodePage::new(1, make_key(1), 1);
            for i in 2..=(n_keys as u64) {
                node.insert_child_page(make_key(i), i as u32);
            }
            assert_eq!(node.keys.len(), n_keys);

            let (left, right) = node.split();
            let diff = (left.keys.len() as i64 - right.keys.len() as i64).unsigned_abs();
            assert!(
                diff <= 1,
                "n_keys={n_keys}: left {} keys, right {} keys — diff {diff} > 1",
                left.keys.len(),
                right.keys.len()
            );
            // Invariants must hold
            assert_eq!(left.keys.len() + 1, left.edges.len());
            assert_eq!(right.keys.len() + 1, right.edges.len());
            // Total keys accounted for (left + promoted + right = original)
            assert_eq!(left.keys.len() + 1 + right.keys.len(), n_keys);
        }
    }

    /// Measure the CBOR framing overhead for OverflowPage and LeafNodePage structures.
    ///
    /// Run with `cargo test measure_cbor_framing -- --nocapture` to see byte counts.
    /// These values inform the derived overflow constants in btree.rs.
    ///
    /// # How to re-tune the constants
    ///
    /// Run this test and check the printed values. For each constant in btree.rs:
    ///
    /// - `OVERFLOW_PAGE_FRAMING_BYTES`: must be >= `overflow_with_cont_large` + 2
    ///   (the +2 accounts for the content byte-string header growing from 1 to 3 bytes
    ///   when content exceeds 255 bytes; the test only measures empty content).
    ///
    /// - `LEAF_PAGE_BASE_FRAMING_BYTES`: must be >= `leaf_empty`.
    ///
    /// - `CELL_FRAMING_BYTES`: must be >= `max_data_cell_overhead` printed below.
    ///   This is measured by sweeping value sizes across each CBOR length-prefix tier
    ///   boundary (0, 23, 24, 255, 256 bytes). The value-header length prefix jumps at
    ///   these boundaries (1→2→3 bytes). The key header also varies with key length,
    ///   but data-tree keys are always 8-byte rowids (header stays 1 byte).
    ///   Index-tree keys can be longer, but index values are always empty so they
    ///   never trigger overflow and are not subject to CHUNK_THRESHOLD.
    #[test]
    fn measure_cbor_framing_overhead() {
        fn cbor_size<T: serde::Serialize>(v: &T) -> usize {
            let mut buf = vec![];
            ciborium::ser::into_writer(v, &mut buf).unwrap();
            buf.len()
        }

        // cell_overhead: everything in the CBOR cell except the CBOR-encoded values array.
        // = 1 (outer array header) + key_header + key_data
        // For data-tree cells key is always 8 bytes (u64 rowid); key_header is 1 byte.
        // The values field is a CBOR array; its header is part of the values encoding.
        let cell_overhead = |values: &Vec<ScalarValue>| -> usize {
            let values_cbor_size = cbor_size(values);
            let cell = Cell::new(vec![0u8; 8], values.clone(), None);
            cbor_size(&cell) - values_cbor_size
        };

        let overflow_no_cont = NodePage::OverflowPage(OverflowPage::new(vec![], None));
        let overflow_with_cont_small = NodePage::OverflowPage(OverflowPage::new(vec![], Some(0)));
        // Worst-case continuation: u32::MAX encodes as 5 CBOR bytes
        let overflow_with_cont_large =
            NodePage::OverflowPage(OverflowPage::new(vec![], Some(u32::MAX)));
        let leaf_empty = NodePage::Leaf(LeafNodePage::default());
        // Measure a leaf with one empty cell to get per-cell framing overhead
        let mut leaf_one_cell = LeafNodePage::default();
        leaf_one_cell.insert_item_at_index(0, Cell::new(vec![0u8; 8], vec![], None));
        let leaf_one_cell_page = NodePage::Leaf(leaf_one_cell);

        let overflow_no_cont_size = cbor_size(&overflow_no_cont);
        let overflow_with_cont_small_size = cbor_size(&overflow_with_cont_small);
        let overflow_with_cont_large_size = cbor_size(&overflow_with_cont_large);
        let leaf_empty_size = cbor_size(&leaf_empty);
        let leaf_one_cell_size = cbor_size(&leaf_one_cell_page);

        // Sweep values of different types; overhead is always key+header framing = 10.
        let sample_values: Vec<Vec<ScalarValue>> = vec![
            vec![],
            vec![ScalarValue::Integer(0)],
            vec![ScalarValue::Integer(i64::MAX)],
            vec![ScalarValue::String("x".repeat(256))],
            vec![ScalarValue::String("x".repeat(65535))],
            vec![ScalarValue::Null, ScalarValue::Boolean(true)],
        ];
        let max_data_cell_overhead = sample_values
            .iter()
            .map(|v| cell_overhead(v))
            .max()
            .unwrap();

        println!(
            "overflow_no_cont (base framing): {} bytes",
            overflow_no_cont_size
        );
        println!(
            "overflow_with_cont Some(0) (min u32): {} bytes",
            overflow_with_cont_small_size
        );
        println!(
            "overflow_with_cont Some(u32::MAX) (max u32): {} bytes",
            overflow_with_cont_large_size
        );
        println!("leaf_empty (base leaf framing): {} bytes", leaf_empty_size);
        println!(
            "per-cell framing (leaf_one_cell - leaf_empty): {} bytes",
            leaf_one_cell_size - leaf_empty_size
        );
        println!(
            "max_data_cell_overhead (key=8, various values): {} bytes",
            max_data_cell_overhead
        );

        // ── Assertions ──────────────────────────────────────────────────────────────
        // Each constant in btree.rs must be >= the measured worst case.

        // OVERFLOW_PAGE_FRAMING_BYTES = 44 must cover:
        //   base framing with empty content + Some(u32::MAX)  →  measured here
        //   + 2 bytes for content byte-string header growing 1→3 when content > 255 bytes
        //     (not measured here since content is empty; accounted for in the constant)
        assert!(
            overflow_with_cont_large_size + 2 <= 44,
            "overflow worst-case framing ({} + 2 content-header growth) exceeds OVERFLOW_PAGE_FRAMING_BYTES=44",
            overflow_with_cont_large_size
        );
        // LEAF_PAGE_BASE_FRAMING_BYTES = 15 must be >= leaf_empty_size
        assert!(
            leaf_empty_size <= 15,
            "leaf_empty framing ({leaf_empty_size}) exceeds constant LEAF_PAGE_BASE_FRAMING_BYTES=15"
        );
        // CELL_FRAMING_BYTES = 10: overhead = cbor_size(cell) - cbor_size(values).
        // For an 8-byte key: 1 (outer array) + 1 (key header) + 8 (key data) = 10.
        // The values CBOR array header is not counted here — it belongs to the values.
        assert!(
            max_data_cell_overhead <= 10,
            "max data-cell overhead ({max_data_cell_overhead}) exceeds constant CELL_FRAMING_BYTES=10"
        );
    }

    /// Verify that `NodePage::cbor_size_estimate` is always ≥ the real CBOR size for
    /// leaf pages across a range of value types, including large integers.
    #[test]
    fn test_leaf_cbor_size_estimate_is_upper_bound() {
        fn real_cbor_size(page: &NodePage) -> usize {
            let mut buf = vec![];
            ciborium::ser::into_writer(page, &mut buf).unwrap();
            buf.len()
        }

        let cases: Vec<Vec<ScalarValue>> = vec![
            vec![],
            vec![ScalarValue::Null],
            vec![ScalarValue::Boolean(true)],
            vec![ScalarValue::Integer(0)],
            vec![ScalarValue::Integer(23)],
            vec![ScalarValue::Integer(24)],        // 2-byte CBOR
            vec![ScalarValue::Integer(255)],
            vec![ScalarValue::Integer(256)],       // 3-byte CBOR
            vec![ScalarValue::Integer(65535)],
            vec![ScalarValue::Integer(65536)],     // 5-byte CBOR
            vec![ScalarValue::Integer(i64::MAX)],  // 9-byte CBOR
            vec![ScalarValue::Integer(i64::MIN)],  // 9-byte CBOR (negative large)
            vec![ScalarValue::Integer(-1)],
            vec![ScalarValue::Integer(-24)],
            vec![ScalarValue::Integer(-256)],
            vec![ScalarValue::Integer(-65536)],
            vec![ScalarValue::Floating(3.14)],
            vec![ScalarValue::String("x".repeat(23))],
            vec![ScalarValue::String("x".repeat(24))],
            vec![ScalarValue::String("x".repeat(255))],
            vec![ScalarValue::String("x".repeat(256))],
            vec![
                ScalarValue::Integer(i64::MAX),
                ScalarValue::String("hello".to_string()),
                ScalarValue::Null,
            ],
        ];

        for values in cases {
            let mut leaf = LeafNodePage::default();
            leaf.insert_item_at_index(0, Cell::new(vec![0u8; 8], values.clone(), None));
            let page = NodePage::Leaf(leaf);
            let estimate = page.cbor_size_estimate();
            let actual = real_cbor_size(&page);
            assert!(
                estimate >= actual,
                "estimate ({estimate}) < actual ({actual}) for values {values:?}"
            );
        }

        // Many small cells: triggers CBOR array-header growth from 1 byte (≤ 23 elements)
        // to 2 bytes (24–255) and 3 bytes (256+).  This is the index-page scenario.
        for cell_count in [1usize, 23, 24, 100, 255, 256] {
            let mut leaf = LeafNodePage::default();
            for i in 0..cell_count {
                leaf.insert_item_at_index(
                    i,
                    Cell::new(vec![0u8; 8], vec![ScalarValue::Integer(i as i64)], None),
                );
            }
            let page = NodePage::Leaf(leaf);
            let estimate = page.cbor_size_estimate();
            let actual = real_cbor_size(&page);
            assert!(
                estimate >= actual,
                "estimate ({estimate}) < actual ({actual}) for {cell_count} cells"
            );
        }
    }

    /// Regression: estimator previously hardcoded 8-byte keys, undercounting pages
    /// where index cells carry long composite keys (e.g. 51 bytes).
    ///
    /// Parametric form tests the general fix; the exact CBOR binary that triggered
    /// the flush panic (136 cells, 51-byte keys, 7495 encoded bytes) is stored in
    /// tests/fixtures/panic_page_cbor.bin and loaded with include_bytes!.
    #[test]
    fn test_leaf_cbor_size_estimate_long_keys() {
        fn real_cbor_size(page: &NodePage) -> usize {
            let mut buf = vec![];
            ciborium::ser::into_writer(page, &mut buf).unwrap();
            buf.len()
        }

        // Parametric: various key lengths, including the 51-byte index composite key.
        for key_len in [24usize, 51, 100, 255, 256] {
            for cell_count in [1usize, 50, 136] {
                let mut leaf = LeafNodePage::default();
                for i in 0..cell_count {
                    leaf.insert_item_at_index(i, Cell::new(vec![0u8; key_len], vec![], None));
                }
                let page = NodePage::Leaf(leaf);
                let estimate = page.cbor_size_estimate();
                let actual = real_cbor_size(&page);
                assert!(
                    estimate >= actual,
                    "estimate ({estimate}) < actual ({actual}) for key_len={key_len} cell_count={cell_count}"
                );
            }
        }

        // Exact CBOR page that triggered the flush panic.
        let cbor = include_bytes!("../../tests/fixtures/panic_page_cbor.bin");
        let page: NodePage =
            ciborium::de::from_reader(cbor.as_slice()).expect("failed to decode panic page");
        let estimate = page.cbor_size_estimate();
        let actual = real_cbor_size(&page);
        assert!(
            estimate >= actual,
            "estimate ({estimate}) < actual ({actual}) for panic page (136 cells, 51-byte keys)"
        );
    }


    proptest! {
        #[test]
        fn test_interior_page_split(interior_num_edges in 4u64..150) {
            let num_inserts = interior_num_edges - 2;
            let mut interior_node = InteriorNodePage::new(1, make_key(1), 1);
            for page in 0..num_inserts {
                interior_node.insert_child_page(make_key(page + 2), 1);
            }
            let (left, right) = interior_node.split();
            // Both sides should be within 1 key of each other
            let diff = (left.keys.len() as i64 - right.keys.len() as i64).unsigned_abs();
            prop_assert!(diff <= 1);
        }
    }

    // ── Size-estimate proptests ───────────────────────────────────────────────
    //
    // One proptest per estimation layer, each verifying the invariant:
    //   estimate >= actual_cbor_size
    //
    // Layer hierarchy (bottom → top):
    //   1. scalar_values_cbor_size_estimate  — values inside a cell
    //   2. cell_cbor_size_estimate           — one cell (inline or overflow)
    //   3. LeafNodePage::cbor_size_estimate  — full leaf page
    //   4. InteriorNodePage::cbor_size_estimate — full interior page

    fn arb_scalar_value() -> impl Strategy<Value = ScalarValue> {
        prop_oneof![
            Just(ScalarValue::Null),
            any::<bool>().prop_map(ScalarValue::Boolean),
            any::<i64>().prop_map(ScalarValue::Integer),
            // Use a bounded finite range — avoids NaN/Inf edge cases in serialization.
            (-1.0e15f64..1.0e15f64).prop_map(ScalarValue::Floating),
            prop::string::string_regex("[a-zA-Z0-9 ]{0,300}").unwrap()
                .prop_map(ScalarValue::String),
            prop::collection::vec(any::<u8>(), 0..=300usize).prop_map(ScalarValue::Blob),
        ]
    }

    /// Layer 1: scalar_values_cbor_size_estimate
    proptest! {
        #[test]
        fn proptest_scalar_values_estimate(
            values in prop::collection::vec(arb_scalar_value(), 0..=20usize)
        ) {
            let estimate = super::scalar_values_cbor_size_estimate(&values);
            let mut buf = vec![];
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            prop_assert!(
                estimate >= buf.len(),
                "estimate={estimate} actual={} values={values:?}", buf.len()
            );
        }
    }

    /// Layer 2: cell_cbor_size_estimate — inline and overflow cells with
    /// arbitrary key lengths (the bug was a hardcoded 8-byte key assumption).
    proptest! {
        #[test]
        fn proptest_cell_estimate(
            key        in prop::collection::vec(any::<u8>(), 0..=300usize),
            values     in prop::collection::vec(arb_scalar_value(), 0..=10usize),
            // Some(inline_bytes, cont_page) → overflow cell; None → inline cell.
            overflow   in prop::option::of(
                (prop::collection::vec(any::<u8>(), 0..=1100usize), any::<u32>())
            ),
        ) {
            let cell = if let Some((inline_bytes, cont)) = overflow {
                Cell::new_overflow(key, inline_bytes, cont)
            } else {
                Cell::new(key, values, None)
            };
            let estimate = super::cell_cbor_size_estimate(&cell);
            let mut buf = vec![];
            ciborium::ser::into_writer(&cell, &mut buf).unwrap();
            prop_assert!(
                estimate >= buf.len(),
                "estimate={estimate} actual={}", buf.len()
            );
        }
    }

    /// Layer 3: LeafNodePage::cbor_size_estimate — arbitrary cells including
    /// long keys (the direct regression layer for the panic-page bug).
    proptest! {
        #[test]
        fn proptest_leaf_page_estimate(
            cells in prop::collection::vec(
                (
                    prop::collection::vec(any::<u8>(), 0..=300usize),
                    prop::collection::vec(arb_scalar_value(), 0..=10usize),
                ),
                0..=50usize,
            ),
        ) {
            let mut leaf = LeafNodePage::default();
            for (i, (key, values)) in cells.into_iter().enumerate() {
                leaf.insert_item_at_index(i, Cell::new(key, values, None));
            }
            let page = NodePage::Leaf(leaf);
            let estimate = page.cbor_size_estimate();
            let mut buf = vec![];
            ciborium::ser::into_writer(&page, &mut buf).unwrap();
            prop_assert!(
                estimate >= buf.len(),
                "estimate={estimate} actual={}", buf.len()
            );
        }
    }

    /// Layer 4: InteriorNodePage::cbor_size_estimate — arbitrary key lengths
    /// and key counts.
    proptest! {
        #[test]
        fn proptest_interior_page_estimate(
            n_extra_keys in 0usize..=50usize,
            key_len      in 1usize..=300usize,
        ) {
            // Build keys as zero-padded buffers whose last N bytes encode the index,
            // ensuring ascending order and uniqueness.
            fn indexed_key(i: usize, len: usize) -> Vec<u8> {
                let mut k = vec![0u8; len];
                let idx = (i as u64).to_be_bytes();
                let copy = len.min(8);
                k[len - copy..].copy_from_slice(&idx[8 - copy..]);
                k
            }
            let mut node = InteriorNodePage::new(0, indexed_key(0, key_len), 1);
            for i in 1..=n_extra_keys {
                node.insert_child_page(indexed_key(i, key_len), (i + 2) as u32);
            }
            let page = NodePage::Interior(node);
            let estimate = page.cbor_size_estimate();
            let mut buf = vec![];
            ciborium::ser::into_writer(&page, &mut buf).unwrap();
            prop_assert!(
                estimate >= buf.len(),
                "estimate={estimate} actual={}", buf.len()
            );
        }
    }
}
