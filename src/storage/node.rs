use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

use super::cell::{Cell, Key};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum NodePage {
    Leaf(LeafNodePage),
    Interior(InteriorNodePage),
    OverflowPage(OverflowPage),
}

impl NodePage {
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

    pub fn interior_mut(&mut self) -> Option<&mut InteriorNodePage> {
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

    pub fn leaf_mut(&mut self) -> Option<&mut LeafNodePage> {
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

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{Cell, NodePage, OverflowPage};

    use super::{InteriorNodePage, LeafNodePage, SearchResult};

    fn make_key(k: u64) -> Vec<u8> {
        k.to_be_bytes().to_vec()
    }

    #[test]
    fn test_insertion_ordering() {
        let mut page = LeafNodePage::default();

        // []
        page.insert_item_at_index(0, Cell::new(make_key(2), vec![0], None));
        // [2]
        page.insert_item_at_index(0, Cell::new(make_key(1), vec![0], None));
        // [1, 2]
        page.insert_item_at_index(2, Cell::new(make_key(3), vec![0], None));
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

        page.insert_item_at_index(0, Cell::new(make_key(1), vec![0], None));
        page.insert_item_at_index(1, Cell::new(make_key(2), vec![0], None));
        page.insert_item_at_index(2, Cell::new(make_key(3), vec![0], None));

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
                SearchResult::NotPresent(i) => {
                    page.insert_item_at_index(i, Cell::new(key.clone(), vec![idx as u8], None))
                }
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
                let value = value.to_be_bytes().to_vec();
                let cell = Cell::new(key_bytes.clone(), value, None);
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

        // cell_overhead: everything in the CBOR cell except the raw value bytes.
        // = 1 (outer array) + key_header + key_data + value_header
        // For data-tree cells key is always 8 bytes (u64 rowid); key_header is 1 byte.
        // value_header grows across tier boundaries: 1 byte (≤23), 2 bytes (24–255), 3 bytes (256+).
        let cell_overhead = |key_len: usize, value_len: usize| -> usize {
            cbor_size(&Cell::new(vec![0u8; key_len], vec![0u8; value_len], None)) - value_len
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

        // Sweep value sizes across CBOR length-prefix tier boundaries for an 8-byte key.
        // The overhead (cell_size - value_len) changes only at tier boundaries.
        let tier_boundaries = [0usize, 23, 24, 255, 256, 65535];
        let max_data_cell_overhead = tier_boundaries
            .iter()
            .map(|&v| cell_overhead(8, v))
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
        println!("--- cell_overhead(key=8, value=N) at tier boundaries ---");
        for v in tier_boundaries {
            println!("  value={v:5}: overhead={}", cell_overhead(8, v));
        }
        println!(
            "max_data_cell_overhead (key=8): {} bytes",
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
        // CELL_FRAMING_BYTES = 13 must be >= max overhead across all value-size tiers
        assert!(
            max_data_cell_overhead <= 13,
            "max data-cell overhead ({max_data_cell_overhead}) exceeds constant CELL_FRAMING_BYTES=13"
        );
    }

    proptest! {
            #[test]
            fn test_interior_page_split(interior_num_edges in 4u64..150) {
                let num_inserts = interior_num_edges-2; // there are already two edges in the interior page
                let mut interior_node = InteriorNodePage::new(1, make_key(1), 1);
                for page in 0..num_inserts {
                    interior_node.insert_child_page(make_key(page+2), 1);
                }
                let (left, right) = interior_node.split();
    // Both sides should be within 1 key of each other
                let diff = (left.keys.len() as i64 - right.keys.len() as i64).unsigned_abs();
                prop_assert!(diff <= 1);
            }
        }
}
