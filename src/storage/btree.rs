use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use std::sync::Arc;

use probe::probe;

use crate::engine::scalarvalue::ScalarValue;
use crate::storage::cell::Cell;
use crate::storage::node::{NodePage, OverflowPage, SearchResult};

use super::btree_verify::VerifyError;
use super::catalog_cache::CatalogSnapshot;
use super::error::Error as StorageError;
use super::node::{self, InteriorNodePage};
use super::node_page_store::NodePageStore;
use super::page_id::PageId;
use super::page_storage::PAGE_SIZE;
use super::{btree_graph, btree_verify, CellReader};

// ── constants ─────────────────────────────────────────────────────────────────

/// Root page of the `db_schema` catalog table.
/// Always page 1 on every database (page 0 is the ZeroPage header).
const CATALOG_ROOT: u32 = 1;

/// Metadata for a single index from the catalog.
/// Cursor position state machine
#[derive(Clone, Debug, PartialEq, Eq)]
enum CursorPosition {
    /// Initial state, or after an operation that leaves cursor unpositioned
    Unpositioned,

    /// Cursor is at a valid leaf cell — fast path, use indices directly
    Valid {
        stack: Vec<InteriorNodeIterator>,
        leaf: LeafNodeIterator,
    },

    /// Position was invalidated by a mutation — lazy seek on next use
    RequiresSeek { saved_key: Vec<u8> },

    /// Iterated past the last key
    AtEnd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CursorState {
    root_page: u32,
    position: CursorPosition,
}

#[derive(Debug, Clone)]
pub struct CursorHandle {
    store: Arc<RefCell<NodePageStore>>,
    state: CursorState,
}

impl CursorHandle {
    pub fn root_page(&self) -> u32 {
        self.state.root_page
    }

    pub fn open_cursor(&mut self) -> Cursor<'_> {
        let store = RefCell::borrow_mut(&self.store);
        Cursor {
            store,
            cursor_state: &mut self.state,
        }
    }
}

pub struct Cursor<'a> {
    store: RefMut<'a, NodePageStore>,
    cursor_state: &'a mut CursorState,
}

/// identifies the page index of the interior node and the index of the child currently selected
type InteriorNodeIterator = (u32, usize);

/// identifies the page index of the leaf node and the index of the entry currently selected
type LeafNodeIterator = (u32, usize);

/// CBOR framing overhead for `NodePage::OverflowPage { content, continuation: Some(u32) }`.
/// Measured empirically — see `measure_cbor_framing_overhead` in node.rs.
///
/// The base framing with empty content and Some(0) is 38 bytes, but the actual overhead
/// grows with both the content size and the continuation page number:
///   - Content byte-string header: 1 byte for empty, 3 bytes for content > 255 bytes (+2)
///   - Continuation u32: 1 byte for values 0-23, up to 5 bytes for values ≥ 65536 (+4)
///
/// Worst-case overhead = 38 + 2 (content header) + 4 (u32 continuation) = 44.
const OVERFLOW_PAGE_FRAMING_BYTES: usize = 44;

/// Maximum bytes stored in a single overflow page.
/// Sized to fill a page while leaving room for worst-case CBOR framing overhead.
/// With PAGE_SIZE=4096: OVERFLOW_LIMIT = 4096 - 44 = 4052.
const OVERFLOW_LIMIT: usize = PAGE_SIZE - OVERFLOW_PAGE_FRAMING_BYTES;

/// Minimum number of cells that must fit on a leaf page to maintain adequate B-tree fill
/// factor after a split. After splitting, each half has ≥ MIN_CELLS_PER_PAGE / 2 cells,
/// which is the minimum for a valid non-root leaf node.
const MIN_CELLS_PER_PAGE: usize = 4;

/// Conservative CBOR framing overhead for a `NodePage::Leaf(LeafNodePage)` with zero cells.
/// Measured empirically — see `measure_cbor_framing_overhead` in node.rs (actual: 14 bytes).
const LEAF_PAGE_BASE_FRAMING_BYTES: usize = 15;

/// CBOR framing overhead per `Cell` (key bytes + all CBOR headers), excluding the CBOR
/// encoding of the values.
///
/// Measured as `cbor_size(cell) - cbor_size(values)` — see `measure_cbor_framing_overhead`
/// in node.rs. Re-tune with:
///   cargo test measure_cbor_framing -- --nocapture
///
/// For data-tree row cells (the only cells whose values can overflow):
///   keys are always 8-byte u64 rowids — key CBOR header is 1 byte (8 ≤ 23).
///   Index-tree keys are larger but index values are always empty, so they never
///   trigger overflow and are not subject to CHUNK_THRESHOLD.
///
/// Overhead breakdown:
///   1  outer CBOR array header
///   1  key byte-string header  (8-byte key ≤ 23 → 1-byte CBOR length prefix)
///   8  key data bytes
///  ──
///  10  total  (value is now a CBOR array, not a byte-string; no wrapper header)
///
/// (Previously 13 when Cell.value was a serde_bytes byte-string with a 3-byte header.)
const CELL_FRAMING_BYTES: usize = 10;

/// Maximum CBOR-encoded size of `values: Vec<ScalarValue>` stored inline in a `Cell`.
/// Derived so that `MIN_CELLS_PER_PAGE` cells can always fit on one page.
/// With PAGE_SIZE=4096: CHUNK_THRESHOLD = (4096 - 15) / 4 - 10 = 1010.
/// Typical SQL rows (< 500 bytes) now store inline with no overflow pages.
const CHUNK_THRESHOLD: usize = (PAGE_SIZE - LEAF_PAGE_BASE_FRAMING_BYTES)
    / MIN_CELLS_PER_PAGE
    - CELL_FRAMING_BYTES;

/// Serialise `values` to a sink that counts bytes without allocating.
/// Returns the exact CBOR encoded length.
fn cbor_size_estimate(values: &[ScalarValue]) -> usize {
    struct CountingWriter(usize);
    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut w = CountingWriter(0);
    ciborium::ser::into_writer(values, &mut w).unwrap();
    w.0
}

/// All cursor operations — both reads and writes use `RefMut<NodePageStore>`.
impl<'a> Cursor<'a> {
    // ── mutation ──────────────────────────────────────────────────────────────

    pub fn insert(&mut self, key: &[u8], values: Vec<ScalarValue>) {
        probe!(database, row_insert);
        // Save current cursor key if positioned, for RequiresSeek after insert
        let saved_cursor_key = match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => {
                let (leaf_page_idx, cell_index) = *leaf;
                let k = {
                    let page = self
                        .store
                        .read(PageId(leaf_page_idx))
                        .expect("read leaf for saved key");
                    match page {
                        NodePage::Leaf(l) if cell_index < l.num_items() => {
                            Some(l.get_key(cell_index))
                        }
                        _ => None,
                    }
                }; // borrow dropped
                k
            }
            _ => None,
        };

        // If the exact encoded size fits inline, skip serialisation entirely.
        // Only overflow rows pay for a second (allocating) serialisation pass.
        let cell = if cbor_size_estimate(&values) > CHUNK_THRESHOLD {
            probe!(database, overflow_write);
            probe!(database, cell_write_overflow);
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            let inline = buf[..CHUNK_THRESHOLD].to_vec();
            let cont_page = split_and_store(&mut *self.store, &buf[CHUNK_THRESHOLD..]);
            Cell::new_overflow(key.to_vec(), inline, cont_page)
        } else {
            probe!(database, cell_write_inline);
            Cell::new(key.to_vec(), values, None)
        };

        let mut stack = Vec::new();
        stack.push(self.cursor_state.root_page);

        loop {
            let top_page_idx = *stack.last().unwrap();

            let search_result = {
                let page = self
                    .store
                    .read(PageId(top_page_idx))
                    .expect("read page in insert search");
                page.search(key)
            }; // borrow dropped

            match search_result {
                SearchResult::Found(insertion_index) => {
                    let mut top_page = self
                        .store
                        .take(PageId(top_page_idx))
                        .expect("take page for insert overwrite");
                    let page_is_leaf = matches!(&top_page, NodePage::Leaf(_));
                    top_page.set_item_at_index(insertion_index, cell);
                    match self.store.write(PageId(top_page_idx), top_page) {
                        Ok(()) => {
                            if page_is_leaf {
                                probe!(database, page_write_leaf, top_page_idx);
                            } else {
                                probe!(database, page_write_interior, top_page_idx);
                            }
                            break;
                        }
                        Err(StorageError::PageFull(top_page)) => {
                            self.split_page(top_page, stack);
                            break;
                        }
                        Err(e) => panic!("Serialization error: {:?}", e),
                    }
                }
                SearchResult::NotPresent(item_idx) => {
                    let mut top_page = self
                        .store
                        .take(PageId(top_page_idx))
                        .expect("take page for insert");
                    let page_is_leaf = matches!(&top_page, NodePage::Leaf(_));
                    top_page.insert_item_at_index(item_idx, cell);
                    match self.store.write(PageId(top_page_idx), top_page) {
                        Ok(()) => {
                            if page_is_leaf {
                                probe!(database, page_write_leaf, top_page_idx);
                            } else {
                                probe!(database, page_write_interior, top_page_idx);
                            }
                            break;
                        }
                        Err(StorageError::PageFull(top_page)) => {
                            self.split_page(top_page, stack);
                            break;
                        }
                        Err(e) => panic!("Serialization error: {:?}", e),
                    }
                }
                SearchResult::GoDown(_child_index, child_page_idx) => {
                    stack.push(child_page_idx);
                }
            }
        }

        // Invalidate cursor position after insert (may cause page splits)
        if let Some(saved_key) = saved_cursor_key {
            self.cursor_state.position = CursorPosition::RequiresSeek { saved_key };
        }
    }

    pub fn insert_u64(&mut self, key: u64, values: Vec<ScalarValue>) {
        self.insert(&encode_u64_key(key), values)
    }

    /// Split an overfull page into two halves and link them into the tree.
    ///
    /// `stack` is the path from root to the overfull page (last element = overfull page).
    ///
    /// Non-root split — insert a new child pointer into the parent:
    ///
    /// ```text
    ///        [parent]              [parent]
    ///           |            =>    /      \
    ///       [overfull]         [left]  [right]
    /// ```
    ///
    /// Root split — the root page index stays stable (the SQLite approach).
    /// Both halves go to fresh pages; the root is rewritten as an interior node:
    ///
    /// ```text
    ///       [root:overfull]       [root:interior]
    ///                        =>    /          \
    ///                          [left]      [right]
    /// ```
    fn split_page(&mut self, overfull_page: NodePage, mut stack: Vec<u32>) {
        probe!(database, page_split);
        let overfull_idx = stack.pop().unwrap();

        let (left_half, right_half) = overfull_page.split();
        let right_first_key = right_half.smallest_key();

        if stack.is_empty() {
            // Root split: allocate both new pages BEFORE any writes.
            let left_idx = self
                .store
                .allocate()
                .expect("allocate left page in root split");
            let right_idx = self
                .store
                .allocate()
                .expect("allocate right page in root split");

            match &left_half {
                NodePage::Leaf(_) => {
                    probe!(database, page_write_leaf, left_idx.as_u32());
                    probe!(database, page_write_leaf, right_idx.as_u32());
                }
                NodePage::Interior(_) => {
                    probe!(database, page_write_interior, left_idx.as_u32());
                    probe!(database, page_write_interior, right_idx.as_u32());
                }
                _ => {}
            }
            self.store
                .write(left_idx, left_half)
                .expect("write left half after root split");
            self.store
                .write(right_idx, right_half)
                .expect("write right half after root split");

            let interior =
                InteriorNodePage::new(left_idx.as_u32(), right_first_key, right_idx.as_u32());
            probe!(database, page_write_interior, overfull_idx);
            self.store
                .write(PageId(overfull_idx), NodePage::Interior(interior))
                .expect("write root interior after split");
        } else {
            // Non-root split: allocate right page first, then write halves.
            let right_idx = self
                .store
                .allocate()
                .expect("allocate right page in non-root split");

            match &left_half {
                NodePage::Leaf(_) => {
                    probe!(database, page_write_leaf, overfull_idx);
                    probe!(database, page_write_leaf, right_idx.as_u32());
                }
                NodePage::Interior(_) => {
                    probe!(database, page_write_interior, overfull_idx);
                    probe!(database, page_write_interior, right_idx.as_u32());
                }
                _ => {}
            }
            self.store
                .write(PageId(overfull_idx), left_half)
                .expect("write left half after non-root split");
            self.store
                .write(right_idx, right_half)
                .expect("write right half after non-root split");

            // Update the parent.
            let parent_idx = stack.pop().unwrap();
            let parent_page = self
                .store
                .take(PageId(parent_idx))
                .expect("take parent page for child pointer update");
            let mut parent_interior = parent_page.interior().unwrap();
            parent_interior.insert_child_page(right_first_key, right_idx.as_u32());
            let parent_node = parent_interior.node();

            probe!(database, page_write_interior, parent_idx);
            match self.store.write(PageId(parent_idx), parent_node) {
                Err(StorageError::PageFull(parent_node)) => {
                    stack.push(parent_idx);
                    self.split_page(parent_node, stack);
                }
                Err(e) => panic!("Serialization error during parent write: {:?}", e),
                Ok(()) => {}
            }
        }
    }

    /// Delete the row at the current cursor position.
    pub fn delete_current(&mut self) {
        probe!(database, row_delete);
        let (leaf_page_idx, cell_index) = match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => *leaf,
            _ => panic!("Cursor must be positioned before delete_current"),
        };

        // Take the page for mutation (single take covers both key read and cell remove).
        let mut page = self
            .store
            .take(PageId(leaf_page_idx))
            .expect("take leaf for delete");

        let deleted_key = match &page {
            NodePage::Leaf(leaf) => leaf.get_key(cell_index),
            _ => panic!("Expected leaf node at cursor position"),
        };

        match &mut page {
            NodePage::Leaf(leaf) => {
                // TODO: Free overflow pages if the deleted cell had them
                // For v1, we accept leaked overflow pages
                leaf.remove_cell(cell_index);
            }
            _ => panic!("Expected leaf node at cursor position"),
        }

        self.store
            .write(PageId(leaf_page_idx), page)
            .expect("Deletion should not cause page overflow");

        self.cursor_state.position = CursorPosition::RequiresSeek {
            saved_key: deleted_key,
        };
    }

    /// Delete a key from the B-tree.
    pub fn delete(&mut self, key: &[u8]) {
        let found = self.find(key);
        if !found {
            return;
        }
        self.delete_current();
    }

    pub fn delete_u64(&mut self, key: u64) {
        self.delete(&encode_u64_key(key))
    }

    // ── navigation ────────────────────────────────────────────────────────────

    fn ensure_positioned(&mut self) -> bool {
        let saved_key = match &self.cursor_state.position {
            CursorPosition::RequiresSeek { saved_key } => Some(saved_key.clone()),
            _ => None,
        };
        if let Some(key) = saved_key {
            self.find(&key)
        } else {
            true
        }
    }

    pub fn first(&mut self) {
        self.select_leftmost_of_idx(self.cursor_state.root_page, Vec::new())
    }

    fn select_leftmost_of_idx(&mut self, page_idx: u32, mut stack: Vec<InteriorNodeIterator>) {
        let mut page_idx = page_idx;
        loop {
            let (is_interior, child_or_count) = {
                let page = self
                    .store
                    .read(PageId(page_idx))
                    .expect("read page in leftmost traversal");
                match page {
                    NodePage::Leaf(l) => (false, l.num_items()),
                    NodePage::Interior(i) => (true, i.get_child_page_by_index(0) as usize),
                    NodePage::OverflowPage(_) => panic!("overflow page in tree traversal"),
                }
            }; // borrow dropped
            if is_interior {
                stack.push((page_idx, 0));
                page_idx = child_or_count as u32;
            } else {
                if child_or_count == 0 {
                    self.cursor_state.position = CursorPosition::AtEnd;
                } else {
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, 0),
                    };
                }
                return;
            }
        }
    }

    fn select_rightmost_of_idx(&mut self, page_idx: u32, mut stack: Vec<InteriorNodeIterator>) {
        let mut page_idx = page_idx;
        loop {
            let (is_interior, last_child_or_count, last_edge) = {
                let page = self
                    .store
                    .read(PageId(page_idx))
                    .expect("read page in rightmost traversal");
                match page {
                    NodePage::Leaf(l) => (false, l.num_items(), 0usize),
                    NodePage::Interior(i) => {
                        let num_edges = i.num_edges();
                        let last = num_edges - 1;
                        (true, i.get_child_page_by_index(last) as usize, last)
                    }
                    NodePage::OverflowPage(_) => panic!("overflow page in tree traversal"),
                }
            }; // borrow dropped
            if is_interior {
                stack.push((page_idx, last_edge));
                page_idx = last_child_or_count as u32;
            } else {
                let num_items = last_child_or_count;
                if num_items == 0 {
                    self.cursor_state.position = CursorPosition::Unpositioned;
                } else {
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, num_items - 1),
                    };
                }
                return;
            }
        }
    }

    pub fn last(&mut self) {
        self.select_rightmost_of_idx(self.cursor_state.root_page, Vec::new())
    }

    pub fn find(&mut self, key: &[u8]) -> bool {
        probe!(database, cursor_find);
        let mut page_idx = self.cursor_state.root_page;
        let mut stack = Vec::new();

        loop {
            let search_result = {
                let page = self
                    .store
                    .read(PageId(page_idx))
                    .expect("read page in find");
                page.search(key)
            }; // borrow dropped

            match search_result {
                SearchResult::Found(index) => {
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, index),
                    };
                    return true;
                }
                SearchResult::NotPresent(index) => {
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, index),
                    };
                    return false;
                }
                SearchResult::GoDown(c_idx, c) => {
                    stack.push((page_idx, c_idx));
                    page_idx = c;
                }
            }
        }
    }

    pub fn find_u64(&mut self, key: u64) -> bool {
        self.find(&encode_u64_key(key))
    }

    #[allow(dead_code)]
    fn row_key(&mut self) -> Option<u64> {
        let cell = self.get_entry()?;
        Some(decode_u64_key(cell.key()))
    }

    pub fn get_entry(&mut self) -> Option<CellReader> {
        let _ = self.ensure_positioned();
        match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => {
                let (leaf_page_number, entry_index) = *leaf;
                CellReader::new(&mut *self.store, leaf_page_number, entry_index)
            }
            _ => None,
        }
    }

    pub fn next(&mut self) {
        probe!(database, cursor_next);
        let needs_reseek = matches!(
            self.cursor_state.position,
            CursorPosition::RequiresSeek { .. }
        );

        let key_found = self.ensure_positioned();

        if needs_reseek && !key_found {
            if let CursorPosition::Valid { leaf, .. } = &self.cursor_state.position {
                let (page_idx, cell_index) = *leaf;
                let is_past_end = {
                    let page = self
                        .store
                        .read(PageId(page_idx))
                        .expect("read leaf in next() post-seek check");
                    page.leaf()
                        .map(|l| cell_index >= l.num_items())
                        .unwrap_or(true)
                }; // borrow dropped
                if is_past_end {
                    self.cursor_state.position = CursorPosition::AtEnd;
                }
            }
            return;
        }

        let next_idx = |current: usize, count| {
            if current + 1 < count {
                Some(current + 1)
            } else {
                None
            }
        };

        let select_first_in_direction = Self::select_leftmost_of_idx;
        self.move_in_direction(next_idx, select_first_in_direction);
    }

    pub fn prev(&mut self) {
        probe!(database, cursor_prev);
        self.ensure_positioned();

        let next_idx = |current: usize, _count| {
            if current != 0 {
                Some(current - 1)
            } else {
                None
            }
        };

        let select_first_in_direction = Self::select_rightmost_of_idx;
        self.move_in_direction(next_idx, select_first_in_direction);
    }

    fn move_in_direction(
        &mut self,
        next_idx: impl Fn(usize, usize) -> Option<usize>,
        select_first_in_direction: impl Fn(&mut Self, u32, Vec<InteriorNodeIterator>),
    ) {
        let (mut stack, leaf) = match &self.cursor_state.position {
            CursorPosition::Valid { stack, leaf } => (stack.clone(), *leaf),
            _ => return,
        };

        let (page_number, entry_index) = leaf;

        let next_entry = {
            let page = self
                .store
                .read(PageId(page_number))
                .expect("read leaf in move_in_direction");
            let num_items = page.leaf().expect("leaf at cursor position").num_items();
            next_idx(entry_index, num_items)
        }; // borrow dropped

        if let Some(entry_index) = next_entry {
            self.cursor_state.position = CursorPosition::Valid {
                stack,
                leaf: (page_number, entry_index),
            };
            return;
        }

        loop {
            if stack.is_empty() {
                self.cursor_state.position = CursorPosition::AtEnd;
                return;
            }

            let (current_interior_idx, current_edge) = stack.pop().unwrap();

            let next_edge_and_child = {
                let node = self
                    .store
                    .read(PageId(current_interior_idx))
                    .expect("read interior in move_in_direction");
                match node {
                    NodePage::Interior(i) => {
                        let edge_count = i.num_edges();
                        next_idx(current_edge, edge_count)
                            .map(|next_edge| (next_edge, i.get_child_page_by_index(next_edge)))
                    }
                    _ => panic!("Interior page expected on stack"),
                }
            }; // borrow dropped

            if let Some((next_edge, next_child_page)) = next_edge_and_child {
                stack.push((current_interior_idx, next_edge));
                select_first_in_direction(self, next_child_page, stack);
                return;
            }
        }
    }

    pub fn verify(&mut self) -> Result<(), VerifyError> {
        btree_verify::verify(&mut *self.store, self.cursor_state.root_page)
    }
}

/// Allocate and write all overflow pages for `rest`, returning the page index
/// of the first overflow page.
fn split_and_store(store: &mut NodePageStore, mut rest: &[u8]) -> u32 {
    assert!(rest.len() > 0);

    let mut page_id = store.allocate().expect("allocate overflow page");
    let first_page_idx = page_id.as_u32();

    while rest.len() > OVERFLOW_LIMIT {
        let next_page_id = store.allocate().expect("allocate overflow page");
        let (first, the_rest) = rest.split_at(OVERFLOW_LIMIT);
        let overflow_page = NodePage::OverflowPage(OverflowPage::new(
            first.to_owned(),
            Some(next_page_id.as_u32()),
        ));
        probe!(database, page_write_overflow, page_id.as_u32());
        store
            .write(page_id, overflow_page)
            .expect("write overflow page");
        rest = the_rest;
        page_id = next_page_id;
    }

    let overflow_page = NodePage::OverflowPage(OverflowPage::new(rest.to_owned(), None));
    probe!(database, page_write_overflow, page_id.as_u32());
    store
        .write(page_id, overflow_page)
        .expect("write last overflow page");

    first_page_idx
}

#[derive(Clone)]
pub struct BTree {
    store: Arc<RefCell<NodePageStore>>,
    /// Maps a table's rootpage to the next rowid to assign for an INSERT.
    /// Shared across `BTree` clones via `Arc` so the catalog instance and the
    /// engine instance (which clones the catalog's `BTree`) see the same values.
    /// Entries are never decreased — only advanced after each write or cleared on
    /// `DROP TABLE` — so the cache stays valid across DELETEs and UPDATEs.
    rowid_cache: Arc<RefCell<HashMap<u32, u64>>>,
    /// In-memory cache of catalog metadata, built lazily on first read and
    /// invalidated on any catalog write.  Uses `RefCell` so read-only lookup
    /// methods can populate the cache without requiring `&mut self`.
    catalog_cache: RefCell<Option<CatalogSnapshot>>,
}

impl BTree {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(path: &str) -> BTree {
        let store = NodePageStore::open(Path::new(path))
            .unwrap_or_else(|e| panic!("Failed to open database: {e}"));
        let is_new = store.page_count() == 0;
        let mut btree = BTree {
            store: Arc::new(RefCell::new(store)),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
            catalog_cache: RefCell::new(None),
        };
        if !is_new {
            btree
                .store
                .borrow()
                .validate_format_version()
                .unwrap_or_else(|e| panic!("{e}"));
        } else {
            // Fresh database: bootstrap the catalog tree.
            btree.bootstrap_catalog();
        }
        btree
    }

    pub fn new_in_memory() -> BTree {
        let store = NodePageStore::new_in_memory();
        let mut btree = BTree {
            store: Arc::new(RefCell::new(store)),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
            catalog_cache: RefCell::new(None),
        };
        btree.bootstrap_catalog();
        btree
    }

    /// Create a `BTree` backed by a custom `PageStorage` implementation.
    ///
    /// If the storage is empty (`page_count() == 0`), bootstraps a fresh
    /// catalog tree. If the storage already contains data, validates the
    /// format version.
    pub fn with_storage(storage: impl super::page_storage::PageStorage + 'static) -> BTree {
        let store = NodePageStore::with_storage(Box::new(storage));
        let is_new = store.page_count() == 0;
        let mut btree = BTree {
            store: Arc::new(RefCell::new(store)),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
            catalog_cache: RefCell::new(None),
        };
        if !is_new {
            btree
                .store
                .borrow()
                .validate_format_version()
                .unwrap_or_else(|e| panic!("{e}"));
        } else {
            btree.bootstrap_catalog();
        }
        btree
    }

    /// Bootstrap a fresh database by creating the catalog B-tree (root page 1)
    /// and inserting the self-referencing `db_schema` entry.
    fn bootstrap_catalog(&mut self) {
        let root = self.create_tree();
        assert_eq!(root, CATALOG_ROOT, "catalog root must always be page 1");
        self.insert_entry(
            "table",
            "db_schema",
            "db_schema",
            root,
            "CREATE TABLE db_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT)",
        );
    }

    /// Create a BTree sharing the underlying page store and rowid cache via Arc,
    /// but with a fresh empty catalog cache. Used by the engine so it doesn't
    /// inherit a deep-copied CatalogSnapshot from the caller.
    pub fn share(&self) -> BTree {
        BTree {
            store: self.store.clone(),
            rowid_cache: self.rowid_cache.clone(),
            catalog_cache: RefCell::new(None),
        }
    }

    /// Invalidate the rowid cache entry for a given rootpage (call on DROP TABLE).
    pub fn invalidate_rowid_cache(&self, rootpage: u32) {
        self.rowid_cache.borrow_mut().remove(&rootpage);
    }

    /// Look up the cached next rowid for a rootpage. Returns None on cache miss.
    pub fn get_cached_next_rowid(&self, rootpage: u32) -> Option<u64> {
        self.rowid_cache.borrow().get(&rootpage).copied()
    }

    /// Store the next rowid for a rootpage in the cache.
    pub fn set_cached_next_rowid(&self, rootpage: u32, next_rowid: u64) {
        self.rowid_cache.borrow_mut().insert(rootpage, next_rowid);
    }

    pub(super) fn store_mut<'a>(&'a self) -> RefMut<'a, NodePageStore> {
        self.store.borrow_mut()
    }

    pub fn open(&self, root_page: u32) -> CursorHandle {
        let state = CursorState {
            root_page,
            position: CursorPosition::Unpositioned,
        };

        CursorHandle {
            store: self.store.clone(),
            state,
        }
    }

    /// Get the total number of pages in the database file.
    pub fn file_size_pages(&self) -> u32 {
        self.store.borrow().page_count()
    }

    /// Create a new tree, returning its root page number.
    pub fn create_tree(&mut self) -> u32 {
        let mut store = self.store.borrow_mut();
        let id = store.allocate().expect("allocate root page");
        let empty_root_node = node::NodePage::Leaf(node::LeafNodePage::default());
        store
            .write(id, empty_root_node)
            .expect("write empty root node");
        id.as_u32()
    }

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        let _ = message;
        // Pager::debug removed in item 131; stub retained to avoid test breakage.
    }

    // ── Catalog API ───────────────────────────────────────────────────────────

    /// Insert a new entry (table or index) into the catalog.
    pub fn insert_entry(
        &mut self,
        obj_type: &str,
        name: &str,
        tbl_name: &str,
        rootpage: u32,
        sql: &str,
    ) {
        let key = self.next_catalog_key();
        let row_values = vec![
            ScalarValue::String(obj_type.to_string()),
            ScalarValue::String(name.to_string()),
            ScalarValue::String(tbl_name.to_string()),
            ScalarValue::Integer(rootpage as i64),
            ScalarValue::String(sql.to_string()),
        ];
        let mut cursor = self.open(CATALOG_ROOT);
        cursor.open_cursor().insert_u64(key, row_values);
        *self.catalog_cache.borrow_mut() = None;
    }

    /// Return a read-only snapshot of the catalog, building it on first call
    /// and after any catalog write.  All lookup methods live on the returned
    /// `CatalogSnapshot` so callers use `btree.catalog().lookup_table(name)`.
    pub fn catalog<'a>(&'a self) -> Ref<'a, CatalogSnapshot> {
        probe!(database, catalog_cache_access);
        self.ensure_catalog_cache();
        Ref::map(self.catalog_cache.borrow(), |o| o.as_ref().unwrap())
    }

    /// Delete the catalog entry for a table and all its associated indexes.
    /// Returns `true` if the table entry was found and deleted.
    pub fn delete_entries_for_table(&mut self, table_name: &str) -> bool {
        let mut deleted_any = false;
        let mut cursor = self.open(CATALOG_ROOT);
        let mut c = cursor.open_cursor();
        c.first();
        loop {
            let mut entry = match c.get_entry() {
                None => break,
                Some(reader) => reader,
            };
            let values = entry.decode_as_array();
            if values.len() >= 5 {
                let obj_type = values[0].as_str().unwrap_or("");
                let name = values[1].as_str().unwrap_or("");
                let tbl_name = values[2].as_str().unwrap_or("");
                if (obj_type == "table" && name == table_name)
                    || (obj_type == "index" && tbl_name == table_name)
                {
                    c.delete_current();
                    deleted_any = true;
                    continue;
                }
            }
            c.next();
        }
        *self.catalog_cache.borrow_mut() = None;
        deleted_any
    }

    // ── catalog private helpers ───────────────────────────────────────────────

    fn ensure_catalog_cache(&self) {
        if self.catalog_cache.borrow().is_none() {
            let snapshot = CatalogSnapshot::build(&mut self.store.borrow_mut());
            *self.catalog_cache.borrow_mut() = Some(snapshot);
        }
    }

    fn next_catalog_key(&self) -> u64 {
        let mut cursor = self.open(CATALOG_ROOT);
        let mut c = cursor.open_cursor();
        c.last();
        if let Some(entry) = c.get_entry() {
            decode_u64_key(entry.key()) + 1
        } else {
            0
        }
    }

    /// Invalidate the catalog cache — call after any catalog write.
    pub fn invalidate_catalog_cache(&self) {
        *self.catalog_cache.borrow_mut() = None;
    }

    /// Test-only helper: check whether the catalog cache is populated.
    #[cfg(test)]
    pub fn catalog_cache_populated(&self) -> bool {
        self.catalog_cache.borrow().is_some()
    }

    /// Mutably borrow the underlying NodePageStore for diagnostic / inspection purposes.
    pub fn node_page_store_mut(
        &self,
    ) -> std::cell::RefMut<'_, super::node_page_store::NodePageStore> {
        self.store.borrow_mut()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn dump_to_file(&self, output_path: &std::path::Path) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(output_path)?;
        let mut writer = std::io::BufWriter::new(file);
        write!(writer, "{}", self)?;
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.store
            .borrow_mut()
            .flush()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }
}

impl Display for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        btree_graph::dump(f, self)?;
        Ok(())
    }
}

/// Encode an i64 integer column value to a variable-length B-tree key, preserving sort order.
pub fn encode_integer_key(i: i64) -> Vec<u8> {
    let encoded = (i as u64) ^ 0x8000_0000_0000_0000;
    encoded.to_be_bytes().to_vec()
}

/// Decode a variable-length index key back to the original i64 column value.
pub fn decode_integer_key(bytes: &[u8]) -> i64 {
    let val = u64::from_be_bytes(bytes.try_into().unwrap());
    (val ^ 0x8000_0000_0000_0000) as i64
}

/// Encode a u64 row key as big-endian bytes, preserving sort order.
pub fn encode_u64_key(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

/// Decode a big-endian byte key back to u64.
pub fn decode_u64_key(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
}

/// Encode a single index column value as sortable bytes with a type tag prefix.
pub fn encode_index_value(value: &crate::engine::scalarvalue::ScalarValue) -> Vec<u8> {
    use crate::engine::scalarvalue::ScalarValue;
    match value {
        ScalarValue::Null => vec![0x00],
        ScalarValue::Integer(i) => {
            let mut key = vec![0x01];
            key.extend_from_slice(&encode_integer_key(*i));
            key
        }
        ScalarValue::Floating(f) => {
            // IEEE 754 sortable encoding: flip sign bit (and all bits if negative)
            let bits = f.to_bits();
            let sortable = if bits >> 63 == 0 {
                bits ^ 0x8000_0000_0000_0000
            } else {
                bits ^ 0xFFFF_FFFF_FFFF_FFFF
            };
            let mut key = vec![0x02];
            key.extend_from_slice(&sortable.to_be_bytes());
            key
        }
        ScalarValue::String(s) => {
            // NUL-terminated: ensures 'a' ([0x03,0x61,0x00]) is not a prefix of 'apple'
            let mut key = vec![0x03];
            key.extend_from_slice(s.as_bytes());
            key.push(0x00); // NUL terminator
            key
        }
        _ => panic!("encode_index_value: unsupported type {:?}", value),
    }
}

#[cfg(test)]
mod test {

    use super::{cbor_size_estimate, CursorPosition, CHUNK_THRESHOLD, OVERFLOW_LIMIT};
    use crate::engine::scalarvalue::ScalarValue;
    use crate::storage::BTree;
    use crate::test::TestDb;
    use proptest::prelude::*;

    #[test]
    fn test_create_blank() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        // Test we can take two cursor handles at the same time
        {
            let mut _cursor1 = btree.open(root);
            let mut _cursor2 = btree.open(root);
        }

        // Test the new table is empty
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            assert!(cursor.get_entry().is_none());
        }

        // Test the new table is empty (readwrite path)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.first();
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_create_and_insert() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.insert_u64(
                42,
                vec![
                    ScalarValue::Integer(42),
                    ScalarValue::Integer(255),
                    ScalarValue::Integer(64),
                ],
            );
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let vals = cursor.get_entry().unwrap().decode_as_array();
            assert_eq!(vals[0], ScalarValue::Integer(42));
            assert_eq!(vals[1], ScalarValue::Integer(255));
            assert_eq!(vals[2], ScalarValue::Integer(64));
        }
    }

    #[test]
    fn test_insert_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            for i in 1..10u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.first();
            for i in 1..10u64 {
                let vals = cursor.get_entry().unwrap().decode_as_array();
                assert_eq!(vals[0], ScalarValue::Integer(i as i64));
                cursor.next();
            }
        }

        println!("{}", btree);
    }

    #[test]
    fn test_search_many() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            for i in 1..10u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.find_u64(7);

            for i in 7..10u64 {
                let vals = cursor.get_entry().unwrap().decode_as_array();
                assert_eq!(vals[0], ScalarValue::Integer(i as i64));
                cursor.next();
            }
        }
    }

    #[test]
    fn multi_level_insertion() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        cursor.insert_u64(1, vec![ScalarValue::String("AA".repeat(263))]);
        cursor.insert_u64(10, vec![ScalarValue::String("BBBB".repeat(900))]);
        cursor.insert_u64(11, vec![ScalarValue::String("C".to_string())]);

        cursor.first();
        cursor.verify().unwrap();

        assert_eq!(1, cursor.row_key().unwrap());
        cursor.next();
        assert_eq!(10, cursor.row_key().unwrap());
        cursor.next();
        assert_eq!(11, cursor.row_key().unwrap());
        cursor.next();
        assert!(cursor.row_key().is_none());

        // Must close cursor or we cant print the btree below
        drop(cursor);
        drop(cursor_handle);

        println!("{btree}");
    }

    fn do_test_ordering(
        elements: &[(u64, (char, usize))],
        my_btree: &mut BTree,
        ordering_forwards: bool,
    ) {
        println!("Test: {elements:?}");

        let mut expected_keys: Vec<u64> = Vec::new();

        let root = my_btree.create_tree();

        let mut cursor_handle = my_btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        for (k, (v, len)) in elements.to_owned() {
            cursor.verify().unwrap();
            let value = v.to_string().repeat(len);
            cursor.insert_u64(k, vec![ScalarValue::String(value)]);
            if !expected_keys.contains(&k) {
                expected_keys.push(k);
            }
        }

        cursor.verify().unwrap();

        expected_keys.sort();
        if !ordering_forwards {
            expected_keys.reverse();
        }

        if ordering_forwards {
            cursor.first();
        } else {
            cursor.last();
        }

        // Verify key ordering (the primary purpose of this test).
        for expected_k in expected_keys {
            let entry = cursor.get_entry().unwrap();
            let actual_key = super::decode_u64_key(entry.key());
            assert_eq!(actual_key, expected_k);

            if ordering_forwards {
                cursor.next();
            } else {
                cursor.prev();
            }
        }

        cursor.verify().unwrap();
    }

    #[test]
    fn large_test_case() {
        let large_test_case = [(28, ('A', 976))];

        let test = TestDb::default();
        let mut btree = test.btree;
        do_test_ordering(&large_test_case, &mut btree, true);

        println!("{btree}");
    }

    proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(20))]
        #[test]
        fn test_ordering(ordering: bool, elements in prop::collection::vec(&(50..60u64, &(prop::char::range('A', 'Z'), 500..600usize)), 10..20usize)) {
            let test = TestDb::default();
            let mut btree = test.btree;
            do_test_ordering(elements.as_slice(), &mut btree, ordering);
        }

    }

    proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2))]

        /// Large-scale insert proptest: 50+ random unique keys inserted in
        /// arbitrary order. After all inserts, verify() must pass and a forward
        /// scan must yield keys in strictly ascending order.
        #[test]
        fn test_large_insert_sorted_and_verified(
            mut keys in prop::collection::vec(0u64..100_000, 50..100usize),
        ) {
            let test = TestDb::default();
            let mut btree = test.btree;
            let root = btree.create_tree();

            // Deduplicate so every key is unique
            keys.sort_unstable();
            keys.dedup();

            {
                let mut cursor_handle = btree.open(root);
                let mut cursor = cursor_handle.open_cursor();
                for &k in &keys {
                    cursor.insert_u64(k, vec![ScalarValue::Integer(k as i64)]);
                }
                cursor.verify().unwrap();
            }

            // Scan and check ascending order
            {
                let mut cursor_handle = btree.open(root);
                let mut cursor = cursor_handle.open_cursor();
                cursor.first();
                let mut prev: Option<Vec<u8>> = None;
                loop {
                    match cursor.get_entry() {
                        Some(entry) => {
                            let key = entry.key().to_vec();
                            if let Some(ref p) = prev {
                                prop_assert!(key > *p, "Keys not in ascending order");
                            }
                            prev = Some(key);
                            cursor.next();
                        }
                        None => break,
                    }
                }
            }
        }
    }

    #[test]
    fn test_integer_key_encoding_order() {
        use super::{decode_integer_key, encode_integer_key};
        let values = [-100i64, -1, 0, 1, 100];
        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| encode_integer_key(v)).collect();

        for i in 1..encoded.len() {
            assert!(
                encoded[i - 1] < encoded[i],
                "Keys not ordered: {:?}",
                values
            );
        }

        for (orig, enc) in values.iter().zip(encoded.iter()) {
            assert_eq!(*orig, decode_integer_key(enc));
        }
    }

    #[test]
    fn test_user_table_root_stable_after_splits() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let initial_root = btree.create_tree();

        {
            let mut cursor = btree.open(initial_root);
            let mut c = cursor.open_cursor();
            for i in 0..100u64 {
                c.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        let mut read_cursor = btree.open(initial_root);
        let mut c = read_cursor.open_cursor();
        c.first();
        let mut count = 0;
        while c.get_entry().is_some() {
            count += 1;
            c.next();
        }
        assert_eq!(count, 100, "All 100 rows should be accessible");
    }

    #[test]
    fn test_empty_table_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();

        assert!(
            cursor.get_entry().is_none(),
            "Empty table should return None"
        );
    }

    #[test]
    fn test_duplicate_key_insert() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("first".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("second".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(1);
            let vals = cursor
                .get_entry()
                .expect("Key should exist")
                .decode_as_array();
            assert_eq!(
                vals[0],
                ScalarValue::String("second".to_string()),
                "Value should be overwritten"
            );
        }
    }

    #[test]
    fn test_find_nonexistent_key() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("three".to_string())]);
            cursor.insert_u64(5, vec![ScalarValue::String("five".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(2);
        }
    }

    #[test]
    fn test_cursor_prev_from_middle() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 0..10u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.find_u64(5);
            let vals = cursor
                .get_entry()
                .expect("Key 5 should exist")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::Integer(5));

            cursor.prev();
            let vals = cursor
                .get_entry()
                .expect("Key 4 should exist")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::Integer(4));

            cursor.prev();
            let vals = cursor
                .get_entry()
                .expect("Key 3 should exist")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::Integer(3));
        }
    }

    #[test]
    fn test_large_tree_ordering() {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;

        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut keys: Vec<u64> = (0..200).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
        keys.shuffle(&mut rng);

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for &key in &keys {
                cursor.insert_u64(key, vec![ScalarValue::Integer(key as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            for expected_key in 0..200u64 {
                let vals = cursor
                    .get_entry()
                    .unwrap_or_else(|| panic!("Key {} should exist", expected_key))
                    .decode_as_array();
                assert_eq!(
                    vals[0],
                    ScalarValue::Integer(expected_key as i64),
                    "Keys should be in sorted order"
                );
                cursor.next();
            }

            assert!(cursor.get_entry().is_none(), "Should be at end of tree");
        }
    }

    #[test]
    fn test_cursor_last_single_page() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
            cursor.insert_u64(2, vec![ScalarValue::String("two".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("three".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            let vals = cursor
                .get_entry()
                .expect("Should have last entry")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::String("three".to_string()));
        }
    }

    #[test]
    fn test_cursor_last_multi_level() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 0..100u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            let vals = cursor
                .get_entry()
                .expect("Should have last entry")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::Integer(99));
        }
    }

    #[test]
    fn test_cursor_last_then_prev() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=5u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            for expected in (1..=5u64).rev() {
                let vals = cursor
                    .get_entry()
                    .unwrap_or_else(|| panic!("Should have key {}", expected))
                    .decode_as_array();
                assert_eq!(vals[0], ScalarValue::Integer(expected as i64));
                cursor.prev();
            }

            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_find_returns_true_for_existing() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(42, vec![ScalarValue::String("the answer".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            let found = cursor.find_u64(42);
            assert!(found, "find() should return true for existing key");

            let vals = cursor
                .get_entry()
                .expect("Cursor should be positioned at found key")
                .decode_as_array();
            assert_eq!(vals[0], ScalarValue::String("the answer".to_string()));
        }
    }

    #[test]
    fn test_find_returns_false_for_missing() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("three".to_string())]);
            cursor.insert_u64(5, vec![ScalarValue::String("five".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            let found = cursor.find_u64(2);
            assert!(!found, "find() should return false for non-existent key");
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            let found = cursor.find_u64(4);
            assert!(!found, "find() should return false for non-existent key");
        }
    }

    #[test]
    fn test_btree_delete_single() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(42, vec![ScalarValue::String("hello".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            assert!(cursor.find_u64(42), "Key should exist before delete");
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.delete_u64(42);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            let found = cursor.find_u64(42);
            assert!(!found, "Key should not exist after delete");
        }
    }

    #[test]
    fn test_btree_delete_nonexistent() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(10, vec![ScalarValue::String("ten".to_string())]);
            cursor.insert_u64(20, vec![ScalarValue::String("twenty".to_string())]);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.delete_u64(15);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            assert!(cursor.find_u64(10), "Key 10 should still exist");
            assert!(cursor.find_u64(20), "Key 20 should still exist");
        }
    }

    #[test]
    fn test_btree_delete_from_multi_page() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=200u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.delete_u64(100);
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            assert!(cursor.find_u64(99), "Key 99 should still exist");
            assert!(!cursor.find_u64(100), "Key 100 should be deleted");
            assert!(cursor.find_u64(101), "Key 101 should still exist");
        }
    }

    #[test]
    fn test_btree_delete_then_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=5u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.delete_u64(3);
        }

        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            let mut keys = Vec::new();
            while let Some(entry) = cursor.get_entry() {
                keys.push(entry.key().to_vec());
                cursor.next();
            }

            assert_eq!(
                keys,
                vec![
                    encode_u64_key(1),
                    encode_u64_key(2),
                    encode_u64_key(4),
                    encode_u64_key(5)
                ],
                "Should see all keys except deleted key 3"
            );
        }
    }

    #[test]
    fn test_btree_delete_all() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=10u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=10u64 {
                cursor.delete_u64(i);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            assert!(
                cursor.get_entry().is_none(),
                "Tree should be empty after deleting all keys"
            );
        }
    }

    #[test]
    fn test_cursor_position_states() {
        // Verify cursor position state transitions
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);

        // Initial state should be Unpositioned
        assert_eq!(cursor_handle.state.position, CursorPosition::Unpositioned);

        // Insert some values
        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
            cursor.insert_u64(2, vec![ScalarValue::String("two".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("three".to_string())]);
        }

        use super::encode_u64_key;

        // After first(), should be Valid
        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(1).as_slice()
            );
        }

        // Navigate through all entries
        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            cursor.next(); // Move to key 2
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(2).as_slice()
            );

            cursor.next(); // Move to key 3
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(3).as_slice()
            );

            cursor.next(); // Move past end
            assert_eq!(cursor.cursor_state.position, CursorPosition::AtEnd);
            assert!(cursor.get_entry().is_none());
        }

        // Test insert invalidates position (RequiresSeek)
        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));

            cursor.insert_u64(4, vec![ScalarValue::String("four".to_string())]);
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::RequiresSeek { .. }
            ));

            cursor.next();
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));
        }

        // Test empty tree → AtEnd
        {
            let empty_root = btree.create_tree();
            let mut empty_cursor = btree.open(empty_root);
            let mut cursor = empty_cursor.open_cursor();
            cursor.first();
            assert_eq!(cursor.cursor_state.position, CursorPosition::AtEnd);
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_cbor_size_estimate_is_exact() {
        fn actual_size(values: &[ScalarValue]) -> usize {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(values, &mut buf).unwrap();
            buf.len()
        }

        let cases: Vec<Vec<ScalarValue>> = vec![
            vec![],
            vec![ScalarValue::Null],
            vec![ScalarValue::Boolean(true)],
            vec![ScalarValue::Boolean(false)],
            vec![ScalarValue::Integer(0)],
            vec![ScalarValue::Integer(23)],
            vec![ScalarValue::Integer(24)],
            vec![ScalarValue::Integer(255)],
            vec![ScalarValue::Integer(256)],
            vec![ScalarValue::Integer(i64::MAX)],
            vec![ScalarValue::Integer(i64::MIN)],
            vec![ScalarValue::Integer(-1)],
            vec![ScalarValue::Integer(-24)],
            vec![ScalarValue::Floating(3.14)],
            vec![ScalarValue::String(String::new())],
            vec![ScalarValue::String("x".to_string())],
            vec![ScalarValue::String("x".repeat(23))],
            vec![ScalarValue::String("x".repeat(24))],
            vec![ScalarValue::String("x".repeat(255))],
            vec![ScalarValue::String("x".repeat(256))],
            vec![ScalarValue::String("x".repeat(1000))],
            vec![ScalarValue::Blob(vec![])],
            vec![ScalarValue::Blob(vec![0u8; 100])],
            vec![
                ScalarValue::Integer(1),
                ScalarValue::String("hello".to_string()),
            ],
            vec![
                ScalarValue::Null,
                ScalarValue::Boolean(true),
                ScalarValue::Integer(42),
            ],
        ];

        for case in &cases {
            assert_eq!(cbor_size_estimate(case), actual_size(case), "{:?}", case);
        }
    }

    #[test]
    fn test_measure_cbor_framing() {
        // Verify that a value whose CBOR encoding exactly equals CHUNK_THRESHOLD bytes
        // stores inline (no overflow), and one byte over triggers overflow.
        //
        // ScalarValue uses serde's externally-tagged enum representation, so
        // vec![ScalarValue::String(s)] with s.len() = N (N ≥ 256) encodes as:
        //   1 (array) + 1 (map) + 7 ("String" key) + 3 (string header) + N = N+12 bytes
        // So for CBOR = CHUNK_THRESHOLD: N = CHUNK_THRESHOLD - 12
        // For CBOR = CHUNK_THRESHOLD + 1: N = CHUNK_THRESHOLD - 11
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        // A string whose CBOR encoding as a single-element array = CHUNK_THRESHOLD bytes.
        let inline_str = "x".repeat(CHUNK_THRESHOLD - 12);
        let inline_vals = vec![ScalarValue::String(inline_str.clone())];
        cursor.insert_u64(1, inline_vals.clone());

        cursor.first();
        assert!(
            cursor.get_entry().unwrap().key().len() > 0,
            "entry should have a key"
        );
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, inline_vals, "CHUNK_THRESHOLD fits inline");

        // One extra byte pushes CBOR over CHUNK_THRESHOLD → overflow.
        let overflow_str = "x".repeat(CHUNK_THRESHOLD - 11);
        let overflow_vals = vec![ScalarValue::String(overflow_str.clone())];
        cursor.insert_u64(2, overflow_vals.clone());

        cursor.find_u64(2);
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(
            decoded, overflow_vals,
            "Value over threshold should use overflow"
        );
    }

    #[test]
    fn test_large_overflow() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // String of length OVERFLOW_LIMIT * 3 - 11 encodes as CBOR of OVERFLOW_LIMIT * 3 + 1 bytes
        // (serde externally-tagged enum: 12 bytes overhead for 256+ char strings), spanning
        // multiple overflow pages.
        let large_vals = vec![ScalarValue::String("x".repeat(OVERFLOW_LIMIT * 3 - 11))];

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, large_vals.clone());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let decoded = cursor.get_entry().unwrap().decode_as_array();
            assert_eq!(decoded, large_vals);
        }
    }

    #[test]
    fn test_cursor_next_after_delete() {
        // Delete current key, verify next() lands on correct successor
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=5u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        // Position at key 3, delete it, verify next() gives us 4
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(3);
            cursor.delete_current();

            // After delete, cursor is in RequiresSeek state with saved_key=3
            // next() should call ensure_positioned() -> find(3) -> lands on 4
            cursor.next();
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(4).as_slice()
            );
        }
    }

    #[test]
    fn test_cursor_next_after_delete_last() {
        // Delete the last key, verify next() reaches AtEnd
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=3u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(3);
            cursor.delete_current();
            cursor.next();
            assert_eq!(cursor.cursor_state.position, CursorPosition::AtEnd);
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_cursor_next_after_insert() {
        // Insert during scan, verify iteration continues correctly
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("three".to_string())]);
            cursor.insert_u64(5, vec![ScalarValue::String("five".to_string())]);
        }

        // Position at key 1, insert key 2, verify next() lands on 2
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(1);
            cursor.insert_u64(2, vec![ScalarValue::String("two".to_string())]);
            // After insert, cursor is in RequiresSeek state with saved_key=1
            cursor.next();
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(2).as_slice()
            );
        }
    }

    #[test]
    fn test_cursor_survives_split() {
        // Insert enough to trigger page split, verify navigation still works
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=50u64 {
                cursor.insert_u64(i, vec![ScalarValue::String(format!("value_{}", i))]);
            }
        }

        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(10);

            // Insert 50 more keys to force splits
            for i in 51..=100u64 {
                cursor.insert_u64(i, vec![ScalarValue::String(format!("value_{}", i))]);
            }

            // Cursor should still be able to navigate from key 10
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(10).as_slice()
            );
            cursor.next();
            assert_eq!(
                cursor.get_entry().unwrap().key(),
                encode_u64_key(11).as_slice()
            );
        }
    }

    #[test]
    fn test_cursor_delete_all_forward() {
        // Delete every key via first() + loop of delete_current() + next()
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=10u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            while cursor.get_entry().is_some() {
                cursor.delete_current();
                cursor.next();
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_cursor_get_entry_after_mutation() {
        // Verify get_entry() re-seeks after delete
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=3u64 {
                cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);
            }
        }

        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(2);
            cursor.delete_current();

            // get_entry() should trigger ensure_positioned() and find key 3
            let entry = cursor.get_entry().unwrap();
            assert_eq!(entry.key(), encode_u64_key(3).as_slice());
        }
    }

    #[test]
    fn test_cursor_refind_after_insert() {
        // After inserts, first()/next() should navigate correctly
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, vec![ScalarValue::String("value1".to_string())]);
            cursor.insert_u64(2, vec![ScalarValue::String("value2".to_string())]);
            cursor.insert_u64(3, vec![ScalarValue::String("value3".to_string())]);
            cursor.first();

            let entry = cursor.get_entry().expect("Should find first entry");
            assert_eq!(entry.key(), encode_u64_key(1).as_slice());
            cursor.next();
            let entry = cursor.get_entry().expect("Should find second entry");
            assert_eq!(entry.key(), encode_u64_key(2).as_slice());
            cursor.next();
            let entry = cursor.get_entry().expect("Should find third entry");
            assert_eq!(entry.key(), encode_u64_key(3).as_slice());
        }
    }

    // ── Variable-length key tests ─────────────────────────────────────────────

    #[test]
    fn test_variable_length_keys_ordering() {
        // Keys of different lengths should sort lexicographically
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for key in &[b"abc" as &[u8], b"b", b"a", b"ab"] {
                cursor.insert(key, vec![ScalarValue::String("value".to_string())]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            for expected_key in &[b"a" as &[u8], b"ab", b"abc", b"b"] {
                let entry = cursor.get_entry().expect("Should have entry");
                assert_eq!(entry.key(), *expected_key);
                cursor.next();
            }
            assert!(cursor.get_entry().is_none(), "Should be at end");
        }
    }

    #[test]
    fn test_variable_length_keys_long_key() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let long_key: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let short_key: Vec<u8> = b"short".to_vec();
        let medium_key: Vec<u8> = b"medium_length_key_here".to_vec();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert(
                &long_key,
                vec![ScalarValue::String("long_value".to_string())],
            );
            cursor.insert(
                &short_key,
                vec![ScalarValue::String("short_value".to_string())],
            );
            cursor.insert(
                &medium_key,
                vec![ScalarValue::String("medium_value".to_string())],
            );
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            assert!(cursor.find(&long_key), "Long key should be found");
            assert_eq!(cursor.get_entry().unwrap().key(), long_key.as_slice());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            assert!(cursor.find(&short_key), "Short key should be found");
        }
    }

    #[test]
    fn test_variable_length_keys_mixed_lengths() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let keys: Vec<Vec<u8>> = vec![
            vec![0x01],
            vec![0x00, 0x00, 0x00, 0x01],
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
            vec![0x00; 16],
            vec![0xFF],
            vec![0x00, 0xFF],
        ];

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for key in &keys {
                cursor.insert(key, vec![ScalarValue::String("v".to_string())]);
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let mut prev_key: Vec<u8> = vec![];
            let mut count = 0;
            while let Some(entry) = cursor.get_entry() {
                let k = entry.key().to_vec();
                assert!(k > prev_key, "Keys must be strictly increasing");
                prev_key = k;
                count += 1;
                cursor.next();
            }
            assert_eq!(count, keys.len(), "All keys should be present");
        }
    }

    #[test]
    fn test_variable_length_keys_splits() {
        // Insert many variable-length keys to trigger B-tree splits
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let mut keys: Vec<Vec<u8>> = (0u32..80)
            .map(|i| {
                let len = 1 + (i as usize % 20);
                let mut k = vec![0u8; len];
                k[0] = (i >> 8) as u8;
                if len > 1 {
                    k[1] = (i & 0xFF) as u8;
                }
                k
            })
            .collect();
        keys.sort();
        keys.dedup();
        let num_keys = keys.len();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for key in &keys {
                cursor.insert(key, vec![ScalarValue::String("val".to_string())]);
            }
            cursor.verify().unwrap();
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let mut count = 0;
            let mut prev_key: Vec<u8> = vec![];
            while let Some(entry) = cursor.get_entry() {
                let k = entry.key().to_vec();
                assert!(k > prev_key, "Keys must be strictly increasing");
                prev_key = k;
                count += 1;
                cursor.next();
            }
            assert_eq!(
                count, num_keys,
                "All keys should be accessible after splits"
            );
        }
    }

    // ── Overflow boundary tests ───────────────────────────────────────────────

    /// Return the number of pages allocated after inserting a value.
    fn page_count(btree: &BTree) -> u32 {
        btree.store.borrow().page_count()
    }

    /// Insert values and return the number of new pages allocated.
    fn overflow_pages_for(btree: &mut BTree, root: u32, key: u64, value: Vec<ScalarValue>) -> u32 {
        let before = page_count(btree);
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(key, value);
        }
        let after = page_count(btree);
        after.saturating_sub(before)
    }

    #[test]
    fn test_chunk_threshold_no_overflow() {
        // vec![ScalarValue::String(s)] with s.len() ≥ 256 encodes as s.len() + 12 CBOR bytes
        // (serde externally-tagged enum: 1 array + 1 map + 7 key + 3 string header).
        // A value with CBOR = CHUNK_THRESHOLD should store inline — no overflow pages.
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let value = vec![ScalarValue::String("x".repeat(CHUNK_THRESHOLD - 12))];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 0,
            "CBOR size = CHUNK_THRESHOLD should store inline (no overflow pages), got {pages_added}"
        );

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_chunk_threshold_plus_one_spills_to_one_overflow_page() {
        // CBOR size = CHUNK_THRESHOLD + 1 should spill to exactly one overflow page
        // (all bytes go to overflow since CHUNK_THRESHOLD+1 < OVERFLOW_LIMIT).
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let value = vec![ScalarValue::String("x".repeat(CHUNK_THRESHOLD - 11))];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 1,
            "CBOR size = CHUNK_THRESHOLD+1 should use exactly 1 overflow page, got {pages_added}"
        );

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_overflow_limit_boundary_two_pages() {
        // CBOR size = OVERFLOW_LIMIT.  With prefix-inline: first CHUNK_THRESHOLD bytes stored
        // in cell, remaining OVERFLOW_LIMIT - CHUNK_THRESHOLD bytes sent to overflow chain.
        // overflow bytes = 4052 - 1010 = 3042 < OVERFLOW_LIMIT → exactly 1 overflow page.
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let value = vec![ScalarValue::String("x".repeat(OVERFLOW_LIMIT - 12))];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 1,
            "overflow bytes = OVERFLOW_LIMIT - CHUNK_THRESHOLD should use 1 overflow page, got {pages_added}"
        );

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_overflow_chain_three_pages() {
        // With prefix-inline: overflow bytes = CBOR size - CHUNK_THRESHOLD.
        // For 3 overflow pages: overflow bytes > 2 * OVERFLOW_LIMIT.
        // String length = CHUNK_THRESHOLD + OVERFLOW_LIMIT*2 - 11
        //   → CBOR = CHUNK_THRESHOLD + OVERFLOW_LIMIT*2 + 1
        //   → overflow bytes = OVERFLOW_LIMIT*2 + 1 → 3 pages.
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let value = vec![ScalarValue::String(
            "x".repeat(CHUNK_THRESHOLD + OVERFLOW_LIMIT * 2 - 11),
        )];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 3,
            "overflow bytes = OVERFLOW_LIMIT*2+1 should create 3 overflow pages, got {pages_added}"
        );

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, value);
    }

    /// Verify that the prefix-inline split is actually happening.
    ///
    /// With old all-overflow: CBOR bytes = CHUNK_THRESHOLD + OVERFLOW_LIMIT → 2 overflow pages.
    /// With new prefix-inline: first CHUNK_THRESHOLD bytes sit in the cell; only OVERFLOW_LIMIT
    /// bytes go to the chain → exactly 1 overflow page.
    ///
    /// If this test fails with pages_added == 2, the inline prefix is not being stored.
    #[test]
    fn test_overflow_prefix_stored_inline() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // CBOR = CHUNK_THRESHOLD + OVERFLOW_LIMIT.
        // overflow bytes = OVERFLOW_LIMIT → exactly 1 overflow page.
        let value = vec![ScalarValue::String(
            "x".repeat(CHUNK_THRESHOLD + OVERFLOW_LIMIT - 12),
        )];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 1,
            "prefix-inline: CBOR = CHUNK_THRESHOLD + OVERFLOW_LIMIT needs 1 overflow page (not 2), got {pages_added}"
        );

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();
        cursor.first();
        let decoded = cursor.get_entry().unwrap().decode_as_array();
        assert_eq!(decoded, value);
    }

    /// Verify that `Cell::inline_bytes` holds exactly CHUNK_THRESHOLD bytes for overflow cells.
    ///
    /// Accesses the raw leaf page via the store to inspect the serialized Cell directly,
    /// without going through CellReader (which reassembles the full buffer).
    #[test]
    fn test_overflow_cell_inline_bytes_length() {
        use crate::storage::node::NodePage;
        use crate::storage::page_id::PageId;

        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Any value with CBOR > CHUNK_THRESHOLD triggers the overflow path.
        let value = vec![ScalarValue::String("x".repeat(CHUNK_THRESHOLD - 11))]; // CBOR = CHUNK_THRESHOLD + 1
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, value);
        }

        // Read the cell directly from the leaf to inspect its inline_bytes.
        let mut store = btree.store.borrow_mut();
        let root_page = store.read(PageId(root)).unwrap().clone();
        let leaf = match root_page {
            NodePage::Leaf(ref l) => l,
            _ => panic!("expected leaf at root"),
        };
        let cell = leaf.get_item_at_index(0).expect("cell 0");
        assert!(
            cell.continuation().is_some(),
            "cell should have a continuation pointer"
        );
        assert_eq!(
            cell.inline_bytes().len(),
            CHUNK_THRESHOLD,
            "inline_bytes must hold exactly CHUNK_THRESHOLD bytes"
        );
    }

    #[test]
    fn with_storage_bootstraps_catalog() {
        use crate::storage::MemoryPageStorage;
        let mut btree = BTree::with_storage(MemoryPageStorage::new());
        assert!(
            btree.catalog().lookup_table_info("db_schema").is_some(),
            "catalog should exist after bootstrap"
        );
    }

    #[test]
    fn with_storage_roundtrip() {
        use crate::db::{execute, ExecuteResult};
        use crate::storage::MemoryPageStorage;
        let mut btree = BTree::with_storage(MemoryPageStorage::new());
        execute("CREATE TABLE t (x INTEGER)", &mut btree).unwrap();
        execute("INSERT INTO t VALUES (99)", &mut btree).unwrap();
        let mut result = match execute("SELECT x FROM t", &mut btree).unwrap() {
            ExecuteResult::Query(q) => q,
            other => panic!("expected Query, got {:?}", other),
        };
        let row = result.next().unwrap();
        assert_eq!(row[0], ScalarValue::Integer(99));
    }
}
