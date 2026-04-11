use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use colored::Colorize;
use probe::probe;

use crate::storage::cell::Cell;
use crate::storage::node::{NodePage, OverflowPage, SearchResult};

use super::btree_verify::VerifyError;
use super::cell::Value;
use super::node::{self, InteriorNodePage};
use super::node_page_store::NodePageStore;
use super::page_id::{self as page_id, PageId};
use super::pager;
use super::{btree_graph, btree_verify, CellReader};

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
pub struct CursorState {
    root_page: u32,
    pub position: CursorPosition,
}

impl CursorState {}

#[derive(Debug, Clone)]
pub struct CursorHandle {
    pub(super) store: Arc<RefCell<NodePageStore>>,
    pub state: CursorState,
}

impl CursorHandle {
    #[allow(dead_code)]
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

    /// Compatibility alias — all cursors use the same mutable accessor now.
    pub fn open_readonly(&mut self) -> Cursor<'_> {
        self.open_cursor()
    }

    /// Compatibility alias — all cursors use the same mutable accessor now.
    pub fn open_readwrite(&mut self) -> Cursor<'_> {
        self.open_cursor()
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
const OVERFLOW_LIMIT: usize = pager::PAGE_SIZE as usize - OVERFLOW_PAGE_FRAMING_BYTES;

/// Minimum number of cells that must fit on a leaf page to maintain adequate B-tree fill
/// factor after a split. After splitting, each half has ≥ MIN_CELLS_PER_PAGE / 2 cells,
/// which is the minimum for a valid non-root leaf node.
const MIN_CELLS_PER_PAGE: usize = 4;

/// Conservative CBOR framing overhead for a `NodePage::Leaf(LeafNodePage)` with zero cells.
/// Measured empirically — see `measure_cbor_framing_overhead` in node.rs (actual: 14 bytes).
const LEAF_PAGE_BASE_FRAMING_BYTES: usize = 15;

/// CBOR framing overhead per `Cell` (key bytes + all CBOR headers), excluding raw value data.
///
/// Measured as `cbor_size(cell) - value_len` across CBOR length-prefix tier boundaries
/// — see `measure_cbor_framing_overhead` in node.rs. Re-tune with:
///   cargo test measure_cbor_framing -- --nocapture
///
/// For data-tree row cells (the only cells whose values can overflow):
///   keys are always 8-byte u64 rowids — key CBOR header is 1 byte (8 ≤ 23).
///   Index-tree keys are larger but index values are always empty, so they never
///   trigger overflow and are not subject to CHUNK_THRESHOLD.
///
/// Overhead breakdown (worst case: value ≥ 256 bytes):
///   1  outer CBOR array header
///   1  key byte-string header  (8-byte key ≤ 23, so 1-byte CBOR length prefix)
///   8  key data bytes
///   3  value byte-string header (value ≥ 256 bytes → 3-byte CBOR length prefix)
///  ──
///  13  total
///
/// Note: with serde_bytes the key header depends only on key LENGTH (not byte values),
/// so high-valued key bytes no longer incur a per-byte varint penalty. Only the
/// value length prefix still requires this tier-based accounting.
const CELL_FRAMING_BYTES: usize = 13;

/// Maximum inline value bytes stored directly in a `Cell` on a leaf page.
/// Derived so that `MIN_CELLS_PER_PAGE` cells can always fit on one page.
/// With PAGE_SIZE=4096: CHUNK_THRESHOLD = (4096 - 15) / 4 - 13 = 1007.
/// Typical SQL rows (< 500 bytes) now store inline with no overflow pages.
const CHUNK_THRESHOLD: usize = (pager::PAGE_SIZE as usize - LEAF_PAGE_BASE_FRAMING_BYTES)
    / MIN_CELLS_PER_PAGE
    - CELL_FRAMING_BYTES;

/// All cursor operations — both reads and writes use `RefMut<NodePageStore>`.
impl<'a> Cursor<'a> {
    #[allow(dead_code)]
    pub fn debug(&self, _message: &str) {
        // Stub: Pager::debug removed in Phase BB item 130.
    }

    // ── mutation ──────────────────────────────────────────────────────────────

    pub fn insert(&mut self, key: &[u8], value: Value) {
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

        // values must be small enough so that a few can fit on each page
        let (first_part, continuation) = if value.len() > CHUNK_THRESHOLD {
            let (first_part, rest) = value.split_at(CHUNK_THRESHOLD);
            probe!(database, overflow_write);
            let second_part = split_and_store(&mut *self.store, rest);
            (first_part.to_owned(), Some(second_part))
        } else {
            (value, None)
        };

        let cell = Cell::new(key.to_vec(), first_part, continuation);

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
                        Err(page_id::Error::PageFull(top_page)) => {
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
                        Err(page_id::Error::PageFull(top_page)) => {
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

    pub fn insert_u64(&mut self, key: u64, value: Value) {
        self.insert(&encode_u64_key(key), value)
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
                Err(page_id::Error::PageFull(parent_node)) => {
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
        store
            .write(page_id, overflow_page)
            .expect("write overflow page");
        rest = the_rest;
        page_id = next_page_id;
    }

    let overflow_page = NodePage::OverflowPage(OverflowPage::new(rest.to_owned(), None));
    store
        .write(page_id, overflow_page)
        .expect("write last overflow page");

    first_page_idx
}

#[derive(Clone)]
pub struct BTree {
    pub(super) store: Arc<RefCell<NodePageStore>>,
    /// Maps a table's rootpage to the next rowid to assign for an INSERT.
    /// Shared across `BTree` clones via `Arc` so the catalog instance and the
    /// engine instance (which clones the catalog's `BTree`) see the same values.
    /// Entries are never decreased — only advanced after each write or cleared on
    /// `DROP TABLE` — so the cache stays valid across DELETEs and UPDATEs.
    rowid_cache: Arc<RefCell<HashMap<u32, u64>>>,
}

impl BTree {
    pub fn new(path: &str) -> BTree {
        let store = NodePageStore::open(Path::new(path))
            .unwrap_or_else(|e| panic!("Failed to open database: {e}"));
        let needs_validate = store.page_count() > 0;
        let btree = BTree {
            store: Arc::new(RefCell::new(store)),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
        };
        if needs_validate {
            btree
                .store
                .borrow()
                .validate_format_version()
                .unwrap_or_else(|e| panic!("{e}"));
        }
        btree
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

    /// Inspect a page and print its raw CBOR structure.
    /// Returns an error if the page number is out of range.
    pub fn inspect_page(&self, page_num: u32) -> Result<(), String> {
        let mut store = self.store.borrow_mut();
        let file_size = store.page_count();

        if page_num >= file_size {
            return Err(format!(
                "Page {} out of range (file has {} pages)",
                page_num, file_size
            ));
        }

        println!(
            "{}",
            format!("Page {} raw CBOR structure:", page_num)
                .bright_cyan()
                .bold()
        );
        println!("{}", "=====================================".bright_black());

        if page_num == 0 {
            let zero = store.get_zero_page().unwrap();
            probe!(database, page_read_zero, 0u32);
            println!("{}: {}", "Type".yellow(), "ZeroPage".green());
            println!("{:#?}", zero);
        } else {
            let node = {
                let n = store.read(PageId(page_num)).map_err(|e| e.to_string())?;
                n.clone()
            }; // borrow dropped
            match &node {
                node::NodePage::Leaf(leaf) => {
                    println!("{}: {}", "Type".yellow(), "LeafNodePage".green());
                    println!("{}: {}", "Number of items".yellow(), leaf.num_items());
                    println!("\n{}:", "Cells".bright_yellow());
                    for i in 0..leaf.num_items() {
                        if let Some(cell) = leaf.get_item_at_index(i) {
                            let key = cell.key();
                            let value = cell.value();
                            let continuation = cell.continuation();

                            let key_hex = key
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");
                            println!("  {}:", format!("Cell {}", i).bright_blue());
                            println!("    {}={}", "key".cyan(), key_hex);
                            println!("    {}={}", "value_len".cyan(), value.len());

                            if let Some(cont_page) = continuation {
                                println!(
                                    "    {}={} {}",
                                    "continuation".cyan(),
                                    cont_page,
                                    "(overflow)".bright_magenta()
                                );
                            } else {
                                println!("    {}={}", "continuation".cyan(), "None".bright_black());
                            }

                            if let Ok(values) = ciborium::de::from_reader::<
                                Vec<crate::engine::scalarvalue::ScalarValue>,
                                _,
                            >(&value[..])
                            {
                                println!("    {}={:?}", "decoded".cyan(), values);
                            } else {
                                let hex: String = value
                                    .iter()
                                    .take(8)
                                    .map(|b| format!("{:02x}", b))
                                    .collect::<Vec<_>>()
                                    .join(" ");
                                let suffix = if value.len() > 8 { "..." } else { "" };
                                println!("    {}={}{}", "hex".cyan(), hex.bright_black(), suffix);
                            }
                        }
                    }
                }
                node::NodePage::Interior(interior) => {
                    println!("{}: {}", "Type".yellow(), "InteriorNodePage".green());
                    println!("{}: {}", "Number of edges".yellow(), interior.num_edges());
                    println!("\n{}:", "Keys and child pages".bright_yellow());
                    for i in 0..interior.num_edges() {
                        let child = interior.get_child_page_by_index(i);
                        if i > 0 {
                            let key = interior.get_key_by_index(i - 1);
                            let key_hex = key
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");
                            println!(
                                "  {}: {}={}, {}={}",
                                format!("Edge {}", i).bright_blue(),
                                "key".cyan(),
                                key_hex,
                                "child_page".cyan(),
                                child
                            );
                        } else {
                            println!(
                                "  {}: {}, {}={}",
                                format!("Edge {}", i).bright_blue(),
                                "(left-most)".bright_black(),
                                "child_page".cyan(),
                                child
                            );
                        }
                    }
                }
                node::NodePage::OverflowPage(overflow) => {
                    println!("{}: {}", "Type".yellow(), "OverflowPage".green());
                    let data = overflow.value();
                    let continuation = overflow.continuation();
                    println!("{}: {}", "Data length".yellow(), data.len());
                    println!("{}: {:?}", "Continuation".yellow(), continuation);
                    let hex: String = data
                        .iter()
                        .take(16)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let suffix = if data.len() > 16 { "..." } else { "" };
                    println!("{}: {}{}", "Data hex".yellow(), hex.bright_black(), suffix);
                }
            }
        }

        Ok(())
    }

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
}

impl Display for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let catalog = crate::catalog::Catalog::from(self.clone());
        btree_graph::dump(f, &catalog)?;
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
            let mut key = vec![0x03];
            key.extend_from_slice(s.as_bytes());
            key.push(0x00);
            key
        }
        _ => panic!("encode_index_value: unsupported type {:?}", value),
    }
}

#[cfg(test)]
mod test {

    use crate::storage::BTree;
    use crate::test::TestDb;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::Read;

    use super::{CursorPosition, CHUNK_THRESHOLD, OVERFLOW_LIMIT};

    #[test]
    fn test_create_blank() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();

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
        let mut btree: BTree = test.catalog.into();

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.insert_u64(42, vec![42, 255, 64]);
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let mut buf = [0; 3];
            cursor.get_entry().unwrap().read(&mut buf).unwrap();
            assert_eq!(&buf, &[42, 255, 64]);
        }

        btree.debug("");
    }

    #[test]
    fn test_insert_many() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert_u64(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.first();
            for i in 1..10u64 {
                let mut buf = [0; 8];
                cursor.get_entry().unwrap().read(&mut buf).unwrap();
                assert_eq!(buf, i.to_be_bytes());
                cursor.next();
            }
        }

        btree.debug("");
        println!("{}", btree);
    }

    #[test]
    fn test_search_many() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();

        let root = btree.create_tree();

        // Test we can insert a value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert_u64(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.find_u64(7);

            for i in 7..10u64 {
                let mut buf = [0; 8];
                cursor.get_entry().unwrap().read(&mut buf).unwrap();
                assert_eq!(buf, i.to_be_bytes());
                cursor.next();
            }
        }

        btree.debug("");
    }

    #[test]
    fn multi_level_insertion() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();

        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        let long_string = |s: &str, num| s.repeat(num).into_bytes();

        cursor.insert_u64(1, long_string("AA", 263));
        cursor.insert_u64(10, long_string("BBBB", 900));
        cursor.debug("");
        cursor.insert_u64(11, long_string("C", 1));

        cursor.first();
        cursor.debug("");
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

        let mut rust_btree = BTreeMap::new();

        let root = my_btree.create_tree();

        let mut cursor_handle = my_btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        for (k, (v, len)) in elements.to_owned() {
            cursor.verify().unwrap();
            let value = v.to_string().repeat(len).as_bytes().to_vec();

            rust_btree.insert(k, value.clone());
            cursor.insert_u64(k, value);
        }

        cursor.verify().unwrap();

        if ordering_forwards {
            cursor.first();
        } else {
            cursor.last();
        }

        let rust_btree_iter: Box<dyn Iterator<Item = _>> = if ordering_forwards {
            Box::new(rust_btree.iter())
        } else {
            Box::new(rust_btree.iter().rev())
        };

        for (_key, actual_value) in rust_btree_iter {
            let mut buf = vec![];
            cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
            assert_eq!(actual_value, &buf);

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
        let mut btree: BTree = test.catalog.into();
        do_test_ordering(&large_test_case, &mut btree, true);

        println!("{btree}");
    }

    proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(20))]
        #[test]
        fn test_ordering(ordering: bool, elements in prop::collection::vec(&(50..60u64, &(prop::char::range('A', 'Z'), 500..600usize)), 10..20usize)) {
            let test = TestDb::default();
            let mut btree: BTree = test.catalog.into();
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
            let mut btree: BTree = test.catalog.into();
            let root = btree.create_tree();

            // Deduplicate so every key is unique
            keys.sort_unstable();
            keys.dedup();

            {
                let mut cursor_handle = btree.open(root);
                let mut cursor = cursor_handle.open_cursor();
                for &k in &keys {
                    cursor.insert_u64(k, k.to_be_bytes().to_vec());
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
        let mut btree: BTree = test.catalog.into();

        let initial_root = btree.create_tree();

        {
            let mut cursor = btree.open(initial_root);
            let mut c = cursor.open_cursor();
            for i in 0..100u64 {
                let value = format!("[{}, \"row_{}\"]", i, i);
                c.insert_u64(i, value.into_bytes());
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
        let mut btree: BTree = test.catalog.into();
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"first".to_vec());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"second".to_vec());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.find_u64(1);
            let mut buf = vec![];
            cursor
                .get_entry()
                .expect("Key should exist")
                .read_to_end(&mut buf)
                .unwrap();
            assert_eq!(buf, b"second", "Value should be overwritten");
        }
    }

    #[test]
    fn test_find_nonexistent_key() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
            cursor.insert_u64(5, b"five".to_vec());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 0..10u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();

            cursor.find_u64(5);
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 5 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 5);

            cursor.prev();
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 4 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 4);

            cursor.prev();
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 3 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 3);
        }
    }

    #[test]
    fn test_large_tree_ordering() {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;

        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let mut keys: Vec<u64> = (0..200).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
        keys.shuffle(&mut rng);

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for &key in &keys {
                cursor.insert_u64(key, key.to_be_bytes().to_vec());
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();

            for expected_key in 0..200u64 {
                let mut buf = [0u8; 8];
                cursor
                    .get_entry()
                    .unwrap_or_else(|| panic!("Key {} should exist", expected_key))
                    .read(&mut buf)
                    .unwrap();
                let actual_key = u64::from_be_bytes(buf);
                assert_eq!(actual_key, expected_key, "Keys should be in sorted order");
                cursor.next();
            }

            assert!(cursor.get_entry().is_none(), "Should be at end of tree");
        }
    }

    #[test]
    fn test_cursor_last_single_page() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(2, b"two".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            let mut buf = vec![];
            cursor
                .get_entry()
                .expect("Should have last entry")
                .read_to_end(&mut buf)
                .unwrap();
            assert_eq!(buf, b"three");
        }
    }

    #[test]
    fn test_cursor_last_multi_level() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 0..100u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Should have last entry")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 99);
        }
    }

    #[test]
    fn test_cursor_last_then_prev() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=5u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.last();

            for expected in (1..=5u64).rev() {
                let mut buf = [0u8; 8];
                cursor
                    .get_entry()
                    .unwrap_or_else(|| panic!("Should have key {}", expected))
                    .read(&mut buf)
                    .unwrap();
                assert_eq!(u64::from_be_bytes(buf), expected);
                cursor.prev();
            }

            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_find_returns_true_for_existing() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(42, b"the answer".to_vec());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            let found = cursor.find_u64(42);
            assert!(found, "find() should return true for existing key");

            let mut buf = vec![];
            cursor
                .get_entry()
                .expect("Cursor should be positioned at found key")
                .read_to_end(&mut buf)
                .unwrap();
            assert_eq!(buf, b"the answer");
        }
    }

    #[test]
    fn test_find_returns_false_for_missing() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
            cursor.insert_u64(5, b"five".to_vec());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(42, b"hello".to_vec());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(10, b"ten".to_vec());
            cursor.insert_u64(20, b"twenty".to_vec());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=200u64 {
                let value = format!("value_{}", i).into_bytes();
                cursor.insert_u64(i, value);
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=5u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            for i in 1..=10u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
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
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);

        assert_eq!(cursor_handle.state.position, CursorPosition::Unpositioned);

        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(2, b"two".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
        }

        use super::encode_u64_key;

        {
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            assert!(matches!(
                cursor.cursor_state.position,
                CursorPosition::Valid { .. }
            ));

            cursor.insert_u64(4, b"four".to_vec());
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
    }

    #[test]
    fn test_measure_cbor_framing() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_cursor();

        let exact_threshold = vec![0u8; CHUNK_THRESHOLD];
        cursor.insert_u64(1, exact_threshold.clone());

        cursor.first();
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, exact_threshold, "CHUNK_THRESHOLD fits in one page");
        assert!(
            cursor.get_entry().unwrap().key().len() > 0,
            "entry should have a key"
        );

        let over_threshold = vec![0u8; CHUNK_THRESHOLD + 1];
        cursor.insert_u64(2, over_threshold.clone());

        cursor.find_u64(2);
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(
            buf, over_threshold,
            "Value over threshold should use overflow"
        );
    }

    #[test]
    fn test_large_overflow() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let large_value = vec![42u8; OVERFLOW_LIMIT * 3 + 1];

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.insert_u64(1, large_value.clone());
        }

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_cursor();
            cursor.first();
            let mut buf = Vec::new();
            cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
            assert_eq!(buf, large_value);
        }
    }
}
