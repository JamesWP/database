use std::cell::{Ref, RefCell, RefMut};
use std::io::Write;
use std::sync::Arc;
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use colored::Colorize;
use probe::probe;

use crate::storage::cell::Cell;
use crate::storage::node::{NodePage, OverflowPage, SearchResult};

use super::btree_verify::VerifyError;
use super::cell::Value;
use super::node::{self, InteriorNodePage};
use super::pager::{self, Pager};
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
    position: CursorPosition,
}

impl CursorState {}

#[derive(Debug, Clone)]
pub struct CursorHandle {
    pager: Arc<RefCell<Pager>>,
    state: CursorState,
}

impl CursorHandle {
    #[allow(dead_code)]
    pub fn root_page(&self) -> u32 {
        self.state.root_page
    }

    pub fn open_readonly<'a>(&'a mut self) -> Cursor<'a, Ref<'a, Pager>> {
        let pager = RefCell::borrow(&self.pager);
        Cursor {
            pager,
            cursor_state: &mut self.state,
        }
    }

    pub fn open_readwrite<'a>(&'a mut self) -> Cursor<'a, RefMut<'a, Pager>> {
        let pager = RefCell::borrow_mut(&self.pager);
        Cursor {
            pager,
            cursor_state: &mut self.state,
        }
    }
}

pub struct Cursor<'a, PagerRef> {
    pager: PagerRef,
    cursor_state: &'a mut CursorState,
}

/// identifies the page index of the interior node and the index of the child curently selected
type InteriorNodeIterator = (u32, usize);

/// identifies the page index of the leaf node and the index of the entry curently selected
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

/// Conservative CBOR framing overhead per `Cell` (key bytes + struct wrapper overhead).
/// The key is always 8 bytes (u64 rowid big-endian). Inline value bytes are added on top.
/// Measured empirically — see `measure_cbor_framing_overhead` in node.rs (actual: 11 bytes).
const CELL_FRAMING_BYTES: usize = 15;

/// Maximum inline value bytes stored directly in a `Cell` on a leaf page.
/// Derived so that `MIN_CELLS_PER_PAGE` cells can always fit on one page.
/// With PAGE_SIZE=4096: CHUNK_THRESHOLD = (4096 - 15) / 4 - 15 = 1005.
/// Typical SQL rows (< 500 bytes) now store inline with no overflow pages.
const CHUNK_THRESHOLD: usize = (pager::PAGE_SIZE as usize - LEAF_PAGE_BASE_FRAMING_BYTES)
    / MIN_CELLS_PER_PAGE
    - CELL_FRAMING_BYTES;

/// Mutable cursor implementation
impl<'a, PagerRef> Cursor<'a, PagerRef>
where
    PagerRef: DerefMut<Target = Pager>,
{
    pub fn insert(&mut self, key: &[u8], value: Value) {
        probe!(database, row_insert);
        // Save current cursor key if positioned, for RequiresSeek after insert
        let saved_cursor_key = match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => {
                let (leaf_page_idx, cell_index) = *leaf;
                let page: NodePage = self.pager.get_and_decode(leaf_page_idx);
                probe!(database, page_read_leaf);
                match &page {
                    NodePage::Leaf(leaf) if cell_index < leaf.num_items() => {
                        Some(leaf.get_key(cell_index))
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        // values must be small enough so that a few can fit on each page
        // this is to ensure when splitting nodes we always end up with at least 50% free space
        let (first_part, continuation) = if value.len() > CHUNK_THRESHOLD {
            let (first_part, rest) = value.split_at(CHUNK_THRESHOLD);
            probe!(database, overflow_write);
            let second_part = split_and_store(&mut self.pager, rest);
            (first_part.to_owned(), Some(second_part))
        } else {
            (value, None)
        };

        let cell = Cell::new(key.to_vec(), first_part, continuation);

        // we maintain a stack of the nodes we decended through in case of needing to split them.
        // Starting at the root, we search to find:
        //   an empty place to put the new value
        //   en existing value to replace
        let mut stack = Vec::new();

        stack.push(self.cursor_state.root_page);

        loop {
            let top_page_idx = *stack.last().unwrap();
            let mut top_page: NodePage = self.pager.get_and_decode(top_page_idx);
            match top_page.search(key) {
                SearchResult::Found(insertion_index) => {
                    probe!(database, page_read_leaf);
                    // We found the index in the node where an existing value for this key exists
                    // we need to replace it with our value

                    top_page.set_item_at_index(insertion_index, cell);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::NotPresent(item_idx) => {
                    probe!(database, page_read_leaf);
                    top_page.insert_item_at_index(item_idx, cell);

                    self.update_page(top_page, stack);

                    break;
                }
                SearchResult::GoDown(_child_index, child_page_idx) => {
                    probe!(database, page_read_interior);
                    // The node does not contain the value, instead we found the index of a child of this node where the value should be inserted instead
                    // we need to go deeper.

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

    /// Updates a page with new content
    ///
    /// # Args
    /// * `stack` the path of pages to the modified page, last entry in the stack is the one which needs updating
    /// * `modified_page` the updated content to be saved to the page identified by the stack
    fn update_page(&mut self, modified_page: NodePage, stack: Vec<u32>) {
        let modified_page_idx = stack.last().unwrap();
        let result = self.pager.encode_and_set(modified_page_idx, &modified_page);

        if result.is_ok() {
            match &modified_page {
                NodePage::Leaf(_) => probe!(database, page_write_leaf),
                NodePage::Interior(_) => probe!(database, page_write_interior),
                _ => {}
            }
            return;
        }

        let result = result.unwrap_err();

        match result {
            pager::EncodingError::NotEnoughSpaceInPage => {
                self.split_page(modified_page, stack);
            }
            pager::EncodingError::SerializationError(e) => {
                panic!("Serialization error: {}", e);
            }
        }
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
    /// The two halves move to new pages; the root is rewritten as an interior node:
    ///
    /// ```text
    ///       [root:overfull]       [root:interior]
    ///                        =>    /          \
    ///                          [left]      [right]
    /// ```
    ///
    fn split_page(&mut self, overfull_page: NodePage, mut stack: Vec<u32>) {
        probe!(database, page_split);
        let overfull_idx = stack.pop().unwrap();

        // 1. Split the overfull page into left and right halves
        let (left_half, right_half) = overfull_page.split();
        let right_idx = self.pager.allocate();
        let right_first_key = right_half.smallest_key();

        // 2. Write both halves to disk (same type as the overfull page)
        match &left_half {
            NodePage::Leaf(_) => {
                probe!(database, page_write_leaf);
                probe!(database, page_write_leaf);
            }
            NodePage::Interior(_) => {
                probe!(database, page_write_interior);
                probe!(database, page_write_interior);
            }
            _ => {}
        }
        self.pager
            .encode_and_set(overfull_idx, left_half)
            .expect("After split, parts are smaller");
        self.pager
            .encode_and_set(right_idx, right_half)
            .expect("After split, parts are smaller");

        // 3. Link the new right page into the tree
        if stack.len() != 0 {
            // Non-root: add a child pointer for the right page to the parent
            let parent_idx = stack.pop().unwrap();
            let parent_page: NodePage = self.pager.get_and_decode(parent_idx);
            probe!(database, page_read_interior);
            let mut parent_interior = parent_page.interior().unwrap();
            parent_interior.insert_child_page(right_first_key, right_idx);
            let parent_node = parent_interior.node();

            probe!(database, page_write_interior);
            let result = self.pager.encode_and_set(parent_idx, parent_node.clone());

            // If the parent is now overfull, recursively split it
            match result {
                Err(pager::EncodingError::NotEnoughSpaceInPage) => {
                    stack.push(parent_idx);
                    self.split_page(parent_node, stack);
                }
                Err(pager::EncodingError::SerializationError(e)) => {
                    panic!("Serialization error: {}", e);
                }
                Ok(_) => {}
            }
        } else {
            // Root: keep the root at the same page index.
            // Move the left half (currently at overfull_idx) to a fresh page,
            // then overwrite the root with a new interior node.
            let left_page: NodePage = self.pager.get_and_decode(overfull_idx);
            match &left_page {
                NodePage::Leaf(_) => probe!(database, page_read_leaf),
                NodePage::Interior(_) => probe!(database, page_read_interior),
                _ => {}
            }
            let left_idx = self.pager.allocate();
            match &left_page {
                NodePage::Leaf(_) => probe!(database, page_write_leaf),
                NodePage::Interior(_) => probe!(database, page_write_interior),
                _ => {}
            }
            self.pager.encode_and_set(left_idx, left_page).unwrap();

            let interior = InteriorNodePage::new(left_idx, right_first_key, right_idx);
            probe!(database, page_write_interior);
            self.pager
                .encode_and_set(overfull_idx, NodePage::Interior(interior))
                .unwrap();
        }
    }

    /// Delete the row at the current cursor position.
    /// The cursor must be positioned (via find, first, next, etc.) before calling.
    ///
    /// After deletion, the cursor enters RequiresSeek state with the deleted key saved.
    /// On next use, it will seek to find the next key >= saved_key (the successor).
    pub fn delete_current(&mut self) {
        // Cursor must be positioned
        let (leaf_page_idx, cell_index) = match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => *leaf,
            _ => panic!("Cursor must be positioned before delete_current"),
        };

        // Read the key before deletion
        let deleted_key = {
            let page: NodePage = self.pager.get_and_decode(leaf_page_idx);
            probe!(database, page_read_leaf);
            match &page {
                NodePage::Leaf(leaf) => leaf.get_key(cell_index),
                _ => panic!("Expected leaf node at cursor position"),
            }
        };

        // Load the leaf page again for mutation
        let mut page: NodePage = self.pager.get_and_decode(leaf_page_idx);
        probe!(database, page_read_leaf);

        // Remove the cell from the leaf
        match &mut page {
            NodePage::Leaf(leaf) => {
                // TODO: Free overflow pages if the deleted cell had them
                // For v1, we accept leaked overflow pages
                leaf.remove_cell(cell_index);
            }
            _ => panic!("Expected leaf node at cursor position"),
        }

        // Write the modified page back
        // Note: We skip rebalancing for v1 - sparse pages are acceptable
        self.pager
            .encode_and_set(leaf_page_idx, page)
            .expect("Deletion should not cause page overflow");

        // Save position for lazy reseek
        self.cursor_state.position = CursorPosition::RequiresSeek {
            saved_key: deleted_key,
        };
    }

    /// Delete a key from the B-tree.
    /// If the key exists, it is removed. If not, this is a no-op.
    pub fn delete(&mut self, key: &[u8]) {
        // Use find to position the cursor on the target leaf
        let found = self.find(key);

        if !found {
            // Key doesn't exist, nothing to delete
            return;
        }

        // Delete at the current cursor position
        self.delete_current();
    }

    pub fn delete_u64(&mut self, key: u64) {
        self.delete(&encode_u64_key(key))
    }
}

/// Imutable cursor implementation
impl<'a, PagerRef> Cursor<'a, PagerRef>
where
    PagerRef: Deref<Target = Pager>,
{
    /// Restore cursor position after a mutation by seeking to the saved key.
    /// If the cursor is in RequiresSeek state, performs a find() to reposition.
    /// Returns true if the saved key was found, false if positioned at insertion point.
    fn ensure_positioned(&mut self) -> bool {
        let saved_key = match &self.cursor_state.position {
            CursorPosition::RequiresSeek { saved_key } => Some(saved_key.clone()),
            _ => None,
        };
        if let Some(key) = saved_key {
            let found = self.find(&key);
            // find() sets position to Valid
            // - If found=true: positioned at the saved key
            // - If found=false: positioned at insertion point (successor of saved key)
            found
        } else {
            // Already positioned, return true to indicate normal case
            true
        }
    }

    /// Move the cursor to point at the first row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// first row to point to
    pub fn first(&mut self) {
        // Take the tree identified by the root page number, and find its left most node and
        // find its smallest entry
        self.select_leftmost_of_idx(self.cursor_state.root_page, Vec::new())
    }

    fn select_leftmost_of_idx(&mut self, page_idx: u32, mut stack: Vec<InteriorNodeIterator>) {
        let mut page_idx = page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);
            match page {
                node::NodePage::Leaf(l) => {
                    probe!(database, page_read_leaf);
                    // We found the first leaf in the tree.
                    if l.num_items() == 0 {
                        // Empty tree
                        self.cursor_state.position = CursorPosition::AtEnd;
                    } else {
                        self.cursor_state.position = CursorPosition::Valid {
                            stack,
                            leaf: (page_idx, 0),
                        };
                    }
                    return;
                }
                node::NodePage::Interior(i) => {
                    probe!(database, page_read_interior);
                    stack.push((page_idx, 0));
                    page_idx = i.get_child_page_by_index(0);
                }
                NodePage::OverflowPage(_) => panic!(),
            }
        }
    }

    fn select_rightmost_of_idx(&mut self, page_idx: u32, mut stack: Vec<InteriorNodeIterator>) {
        let mut page_idx = page_idx;

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);
            match page {
                node::NodePage::Leaf(l) => {
                    probe!(database, page_read_leaf);
                    // We found the rightmost leaf in the tree.
                    if l.num_items() == 0 {
                        // Empty tree
                        self.cursor_state.position = CursorPosition::Unpositioned;
                    } else {
                        self.cursor_state.position = CursorPosition::Valid {
                            stack,
                            leaf: (page_idx, l.num_items() - 1),
                        };
                    }
                    return;
                }
                node::NodePage::Interior(i) => {
                    probe!(database, page_read_interior);
                    stack.push((page_idx, i.num_edges() - 1));
                    page_idx = i.get_child_page_by_index(i.num_edges() - 1);
                }
                NodePage::OverflowPage(_) => panic!(),
            }
        }
    }

    /// Move the cursor to point at the last row in the btree
    /// This may result in the cursor not pointing to a row if there is no
    /// last row to point to
    pub fn last(&mut self) {
        // Take the tree identified by the root page number, and find its right most node and
        // find its largest entry.
        self.select_rightmost_of_idx(self.cursor_state.root_page, Vec::new())
    }

    /// Move the cursor to point at the row in the btree identified by the given key
    /// Returns true if the key was found, false if not found.
    /// When false, the cursor is positioned where the key would be inserted.
    pub fn find(&mut self, key: &[u8]) -> bool {
        probe!(database, cursor_find);
        let mut page_idx = self.cursor_state.root_page;
        let mut stack = Vec::new();

        loop {
            let page: NodePage = self.pager.get_and_decode(page_idx);

            match page.search(key) {
                SearchResult::Found(index) => {
                    probe!(database, page_read_leaf);
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, index),
                    };
                    return true;
                }
                SearchResult::NotPresent(index) => {
                    probe!(database, page_read_leaf);
                    self.cursor_state.position = CursorPosition::Valid {
                        stack,
                        leaf: (page_idx, index),
                    };
                    return false;
                }
                SearchResult::GoDown(c_idx, c) => {
                    probe!(database, page_read_interior);
                    stack.push((page_idx, c_idx));
                    // we should continue searching at the child page below
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

    pub fn get_entry<'b>(&'b mut self) -> Option<CellReader<'b>> {
        let _ = self.ensure_positioned();
        match &self.cursor_state.position {
            CursorPosition::Valid { leaf, .. } => {
                let (leaf_page_number, entry_index) = *leaf;
                CellReader::new(&self.pager, leaf_page_number, entry_index)
            }
            _ => None,
        }
    }

    /// Move the cursor to point at the next item in the btree
    pub fn next(&mut self) {
        probe!(database, cursor_next);
        // Check if we need to reposition due to a mutation
        let needs_reseek = matches!(
            self.cursor_state.position,
            CursorPosition::RequiresSeek { .. }
        );

        let key_found = self.ensure_positioned();

        // If we just repositioned after a mutation:
        // - If key was NOT found (delete case): we're at the successor, don't advance
        // - If key WAS found (insert case): we're at the saved position, advance normally
        if needs_reseek && !key_found {
            // Positioned at insertion point (successor after delete)
            // Check if the position is actually valid (could be past the end)
            if let CursorPosition::Valid { leaf, .. } = &self.cursor_state.position {
                let (page_idx, cell_index) = *leaf;
                let page: NodePage = self.pager.get_and_decode(page_idx);
                probe!(database, page_read_leaf);
                if let Some(leaf) = page.leaf() {
                    if cell_index >= leaf.num_items() {
                        // Positioned past the end
                        self.cursor_state.position = CursorPosition::AtEnd;
                    }
                }
            }
            return;
        }

        // function takes a curent index and the number of indexes, and returns Some(idx) where idx is the next index to consider
        // or none if there are no more on this page
        let next_idx = |curent: usize, count| {
            if curent + 1 < count {
                Some(curent + 1)
            } else {
                None
            }
        };

        // function to move the cursor to the next item to consider in subtree identified by page_idx in the given direction
        let select_first_in_direction = Self::select_leftmost_of_idx;

        self.move_in_direction(next_idx, select_first_in_direction);
    }

    /// Move the cursor to point at the next item in the btree
    pub fn prev(&mut self) {
        probe!(database, cursor_prev);
        self.ensure_positioned();

        // For prev(), we always retreat, regardless of whether we just repositioned
        // This is correct for both delete (at successor, retreat to predecessor)
        // and insert (at saved position, retreat to previous) cases

        // function takes a curent index and the number of indexes, and returns Some(idx) where idx is the next index to consider
        // or none if there are no more on this page
        let next_idx = |curent: usize, _count| {
            if curent != 0 {
                Some(curent - 1)
            } else {
                None
            }
        };

        // function to move the cursor to the next item to consider in subtree identified by page_idx in the given direction
        let select_first_in_direction = Self::select_rightmost_of_idx;

        self.move_in_direction(next_idx, select_first_in_direction);
    }

    fn move_in_direction(
        &mut self,
        next_idx: impl Fn(usize, usize) -> Option<usize>,
        select_first_in_direction: impl Fn(&mut Self, u32, Vec<InteriorNodeIterator>),
    ) {
        // Extract current position
        let (mut stack, leaf) = match &self.cursor_state.position {
            CursorPosition::Valid { stack, leaf } => (stack.clone(), *leaf),
            _ => {
                // If not positioned, no-op
                return;
            }
        };

        let (page_number, entry_index) = leaf;
        let page: NodePage = self.pager.get_and_decode(page_number);
        probe!(database, page_read_leaf);
        let page = page
            .leaf()
            .expect("Values are always supposed to be in leaf pages");
        let num_items_in_leaf = page.num_items();
        if let Some(entry_index) = next_idx(entry_index, num_items_in_leaf) {
            self.cursor_state.position = CursorPosition::Valid {
                stack,
                leaf: (page_number, entry_index),
            };
            return;
        }
        loop {
            // if the stack is empty then we have no more places to go
            if stack.is_empty() {
                self.cursor_state.position = CursorPosition::AtEnd;
                return;
            }

            let (curent_interior_idx, curent_edge) = stack.pop().unwrap();

            let curent_interior: NodePage = self.pager.get_and_decode(curent_interior_idx);
            probe!(database, page_read_interior);
            let curent_interior = curent_interior
                .interior()
                .expect("The stack should only contain interior pages");
            let edge_count = curent_interior.num_edges();

            // if we there are more edges to the right:
            if let Some(next_edge) = next_idx(curent_edge, edge_count) {
                // select the next edge in the curent page
                stack.push((curent_interior_idx, next_edge));

                // find the page_idx for the new edge
                let curent_edge_idx = curent_interior.get_child_page_by_index(next_edge);

                // then select the first item in the leftmost leaf of that subtree
                select_first_in_direction(self, curent_edge_idx, stack);
                return;
            }

            // if there are no more edges in this node:
            //    pop this item off the stack and repeat
            // pop already happened
        }
    }

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        self.pager.debug(message);
    }

    pub fn verify(&self) -> Result<(), VerifyError> {
        btree_verify::verify(&self.pager, self.cursor_state.root_page)
    }
}

fn split_and_store(pager: &mut Pager, mut rest: &[u8]) -> u32 {
    // [first] [next] [next+1] ...
    //  ^ page_idx
    //          ^ next_page_idx

    // [next] [last]
    //  ^ page_idx
    //         ^ next_page_idx

    // after loop exits:
    // [last]
    //  ^ page_idx

    assert!(rest.len() > 0);

    let mut page_idx = pager.allocate();
    let first_page_idx = page_idx;

    while rest.len() > OVERFLOW_LIMIT {
        // We know there will be at least one more page following this...
        let next_page_idx = pager.allocate();
        let (first, the_rest) = rest.split_at(OVERFLOW_LIMIT);
        let overflow_page =
            NodePage::OverflowPage(OverflowPage::new(first.to_owned(), Some(next_page_idx)));
        pager
            .encode_and_set(page_idx, overflow_page)
            .expect("to be able to store overflow pages");
        rest = the_rest;
        page_idx = next_page_idx;
    }

    let overflow_page = NodePage::OverflowPage(OverflowPage::new(rest.to_owned(), None));
    pager
        .encode_and_set(page_idx, overflow_page)
        .expect("to be able to store overflow pages");

    first_page_idx
}

#[derive(Clone)]
pub struct BTree {
    pub(super) pager: Arc<RefCell<pager::Pager>>,
}

impl BTree {
    pub fn new(path: &str) -> BTree {
        let btree = BTree {
            pager: Arc::new(RefCell::new(Pager::new(path))),
        };
        if btree.pager.borrow().get_file_size_pages() > 0 {
            btree.pager.borrow().validate_format_version();
        }
        btree
    }

    pub fn open(&self, root_page: u32) -> CursorHandle {
        let state = CursorState {
            root_page,
            position: CursorPosition::Unpositioned,
        };

        CursorHandle {
            pager: self.pager.clone(),
            state,
        }
    }

    /// Get the total number of pages in the database file.
    pub fn file_size_pages(&self) -> u32 {
        self.pager.borrow().get_file_size_pages()
    }

    /// Create a new tree, returning its root page number.
    pub fn create_tree(&mut self) -> u32 {
        let mut pager = self.pager.borrow_mut();
        let idx = pager.allocate();
        let empty_leaf_node = node::LeafNodePage::default();
        let empty_root_node = node::NodePage::Leaf(empty_leaf_node);
        pager.encode_and_set(idx, empty_root_node).unwrap();
        idx
    }

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        self.pager.borrow().debug(message)
    }

    /// Inspect a page and print its raw CBOR structure.
    /// Returns an error if the page number is out of range.
    pub fn inspect_page(&self, page_num: u32) -> Result<(), String> {
        let pager = self.pager.borrow();
        let file_size = pager.get_file_size_pages();

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
            // ZeroPage
            let zero: pager::ZeroPage = pager.get_and_decode(0);
            probe!(database, page_read_zero);
            println!("{}: {}", "Type".yellow(), "ZeroPage".green());
            println!("{:#?}", zero);
        } else {
            // NodePage (Leaf, Interior, or OverflowPage)
            let node: NodePage = pager.get_and_decode(page_num);
            match &node {
                node::NodePage::Leaf(leaf) => {
                    probe!(database, page_read_leaf);
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

                            // Try to decode as CBOR Vec<ScalarValue>
                            if let Ok(values) = ciborium::de::from_reader::<
                                Vec<crate::engine::scalarvalue::ScalarValue>,
                                _,
                            >(&value[..])
                            {
                                println!("    {}={:?}", "decoded".cyan(), values);
                            } else {
                                // Fall back to hex
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
                    probe!(database, page_read_interior);
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
                    probe!(database, page_read_overflow);
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
///
/// Flips the sign bit so that negative numbers sort before positive numbers
/// in big-endian byte comparison, matching SQL sort order for integers.
///
/// Examples:
///   i64::MIN → [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
///        -1  → [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
///         0  → [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
///         1  → [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
///   i64::MAX → [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
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
///
/// Type tags ensure cross-type ordering: NULL(0x00) < INTEGER(0x01) < REAL(0x02) < TEXT(0x03).
/// TEXT values are NUL-terminated ([0x03][utf8_bytes][0x00]) so that shorter strings are not
/// byte-level prefixes of longer strings. This ensures BlobStartsWith can reliably detect
/// exact column-value equality (e.g. 'a' is not a prefix of 'apple' after NUL termination).
/// The returned bytes are concatenated with other column encodings to form the full index key.
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

        // Test we can take two readonly cursors at the same time
        {
            let mut _cursor1 = btree.open(root);
            let mut _cursor2 = btree.open(root);
        }

        // Test the new table is empty, when using a readonly cursor
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            assert!(cursor.get_entry().is_none());
        }

        // Test the new table is empty, when using a readwrite cursor
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

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
            let mut cursor = cursor_handle.open_readwrite();

            cursor.insert_u64(42, vec![42, 255, 64]);
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert_u64(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();

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
            let mut cursor = cursor_handle.open_readwrite();

            for i in 1..10u64 {
                let value = i.to_be_bytes().to_vec();
                cursor.insert_u64(i, value);
            }
        }

        // Test we can read out the new value
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();

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
        let mut cursor = cursor_handle.open_readwrite();

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
        let mut cursor = cursor_handle.open_readwrite();

        for (k, (v, len)) in elements.to_owned() {
            cursor.verify().unwrap();
            let value = v.to_string().repeat(len).as_bytes().to_vec();

            rust_btree.insert(k, value.clone());
            cursor.insert_u64(k, value);
        }

        cursor.verify().unwrap();
        // cursor.debug("Before order check");

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
            // println!("Key: {key} {my_value}");
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
        #[test]
        fn test_ordering(ordering: bool, elements in prop::collection::vec(&(50..60u64, &(prop::char::range('A', 'Z'), 500..600usize)), 10..20usize)) {
            let test = TestDb::default();
            let mut btree: BTree = test.catalog.into();
            do_test_ordering(elements.as_slice(), &mut btree, ordering);
        }

    }

    proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(5))]

        /// Large-scale insert proptest: 200+ random unique keys inserted in
        /// arbitrary order. After all inserts, verify() must pass and a forward
        /// scan must yield keys in strictly ascending order.
        #[test]
        fn test_large_insert_sorted_and_verified(
            mut keys in prop::collection::vec(0u64..100_000, 200..300usize),
        ) {
            let test = TestDb::default();
            let mut btree: BTree = test.catalog.into();
            let root = btree.create_tree();

            // Deduplicate so every key is unique
            keys.sort_unstable();
            keys.dedup();

            {
                let mut cursor_handle = btree.open(root);
                let mut cursor = cursor_handle.open_readwrite();
                for &k in &keys {
                    cursor.insert_u64(k, k.to_be_bytes().to_vec());
                }
                // Verify structural integrity after all inserts
                cursor.verify().unwrap();
            }

            // Scan and check ascending order
            {
                let mut cursor_handle = btree.open(root);
                let mut cursor = cursor_handle.open_readonly();
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

        // Encoded keys maintain sort order (Vec<u8> compares lexicographically)
        for i in 1..encoded.len() {
            assert!(
                encoded[i - 1] < encoded[i],
                "Keys not ordered: {:?}",
                values
            );
        }

        // Round-trip decode
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(decode_integer_key(&encoded[i]), v);
        }
    }

    #[test]
    fn test_user_table_root_stable_after_splits() {
        // User table root pages should remain stable after many inserts
        // that cause multiple levels of splits.
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();

        let initial_root = btree.create_tree();

        // Insert enough rows to force multiple root splits
        {
            let mut cursor = btree.open(initial_root);
            let mut c = cursor.open_readwrite();
            for i in 0..100u64 {
                let value = format!("[{}, \"row_{}\"]", i, i);
                c.insert_u64(i, value.into_bytes());
            }
        }

        // Data should be accessible through the original rootpage
        let mut read_cursor = btree.open(initial_root);
        let mut c = read_cursor.open_readonly();
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
        // Verify first() on empty tree returns None
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();

        assert!(
            cursor.get_entry().is_none(),
            "Empty table should return None"
        );
    }

    #[test]
    fn test_duplicate_key_insert() {
        // Insert same key twice with different values, verify overwrite
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert key=1 with value "first"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"first".to_vec());
        }

        // Insert key=1 again with value "second"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"second".to_vec());
        }

        // Read back and verify it's "second"
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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
        // find() for key not in tree
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 3, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
            cursor.insert_u64(5, b"five".to_vec());
        }

        // Try to find key 2 (doesn't exist)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.find_u64(2);
            // After find() for non-existent key, cursor should be positioned
            // but get_entry() should return None (or positioned at next key)
            // Current implementation positions at the spot where it would be inserted
            // For now, just verify it doesn't panic
        }
    }

    #[test]
    fn test_cursor_prev_from_middle() {
        // Insert 10 keys, navigate to middle, call prev(), verify correct key
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 0-9
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 0..10u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Navigate to middle and go backwards
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();

            // Find key 5
            cursor.find_u64(5);
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 5 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 5);

            // Move to prev (key 4)
            cursor.prev();
            let mut buf = [0u8; 8];
            cursor
                .get_entry()
                .expect("Key 4 should exist")
                .read(&mut buf)
                .unwrap();
            assert_eq!(u64::from_be_bytes(buf), 4);

            // Move to prev again (key 3)
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
        // Insert 1000+ keys in random order, scan and verify sorted
        use rand::seq::SliceRandom;
        use rand::SeedableRng;

        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Create shuffled keys
        let mut keys: Vec<u64> = (0..1000).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
        keys.shuffle(&mut rng);

        // Insert in random order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for &key in &keys {
                cursor.insert_u64(key, key.to_be_bytes().to_vec());
            }
        }

        // Scan and verify sorted order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            for expected_key in 0..1000u64 {
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

            // Should be at end
            assert!(cursor.get_entry().is_none(), "Should be at end of tree");
        }
    }

    #[test]
    fn test_cursor_last_single_page() {
        // Small tree, last() returns highest key
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 2, 3
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(2, b"two".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
        }

        // Call last() and verify we get key 3
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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
        // Tree with interior nodes, last() returns highest key
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert enough to force splits (creating interior nodes)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 0..100u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Call last() and verify we get key 99
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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
        // last() then prev() navigates backward correctly
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1-5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=5u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // last() should give us 5, then prev() should give us 4, 3, 2, 1
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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

            // After prev from key 1, should be at beginning (None)
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_find_returns_true_for_existing() {
        // find() returns true when key exists
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert key 42
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(42, b"the answer".to_vec());
        }

        // find(42) should return true
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find_u64(42);
            assert!(found, "find() should return true for existing key");

            // Verify we can read the value
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
        // find() returns false when key doesn't exist
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 3, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
            cursor.insert_u64(5, b"five".to_vec());
        }

        // find(2) should return false (2 doesn't exist)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find_u64(2);
            assert!(!found, "find() should return false for non-existent key");
        }

        // find(4) should also return false
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find_u64(4);
            assert!(!found, "find() should return false for non-existent key");
        }
    }

    #[test]
    fn test_btree_delete_single() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert a single key-value pair
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(42, b"hello".to_vec());
        }

        // Verify it exists
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find_u64(42);
            assert!(found, "Key 42 should exist before deletion");
        }

        // Delete the key
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.delete_u64(42);
        }

        // Verify it's gone
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find_u64(42);
            assert!(!found, "Key 42 should not exist after deletion");
        }
    }

    #[test]
    fn test_btree_delete_nonexistent() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert some keys
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(10, b"ten".to_vec());
            cursor.insert_u64(20, b"twenty".to_vec());
        }

        // Try to delete a non-existent key (should be no-op)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.delete_u64(15); // Key 15 doesn't exist
        }

        // Verify original keys still exist
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            assert!(cursor.find_u64(10), "Key 10 should still exist");
            assert!(cursor.find_u64(20), "Key 20 should still exist");
        }
    }

    #[test]
    fn test_btree_delete_from_multi_page() {
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert enough keys to force splits (200 keys should be sufficient)
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=200u64 {
                let value = format!("value_{}", i).into_bytes();
                cursor.insert_u64(i, value);
            }
        }

        // Delete a key from the middle
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.delete_u64(100);
        }

        // Verify key 100 is gone but neighbors exist
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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

        // Insert keys 1, 2, 3, 4, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=5u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Delete key 3
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.delete_u64(3);
        }

        // Scan and verify we see 1, 2, 4, 5 (not 3)
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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

        // Insert keys 1 through 10
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=10u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Delete all keys one by one
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=10u64 {
                cursor.delete_u64(i);
            }
        }

        // Verify tree is empty
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let mut cursor_handle = btree.open(root);

        // Initial state should be Unpositioned
        assert_eq!(cursor_handle.state.position, CursorPosition::Unpositioned);

        // Insert some values
        {
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(2, b"two".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
        }

        use super::encode_u64_key;

        // After first(), should be Valid
        {
            let mut cursor = cursor_handle.open_readonly();
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
            let mut cursor = cursor_handle.open_readonly();
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

        // Test empty tree → AtEnd
        {
            let empty_root = btree.create_tree();
            let mut empty_cursor = btree.open(empty_root);
            let mut cursor = empty_cursor.open_readonly();
            cursor.first();
            assert_eq!(cursor.cursor_state.position, CursorPosition::AtEnd);
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_cursor_next_after_delete() {
        // Delete current key, verify next() lands on correct successor
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 2, 3, 4, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=5u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Position at key 3, delete it, verify next() gives us 4
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 2, 3
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=3u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Delete key 3 (last), verify next() reaches AtEnd
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 3, 5
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(1, b"one".to_vec());
            cursor.insert_u64(3, b"three".to_vec());
            cursor.insert_u64(5, b"five".to_vec());
        }

        // Position at key 1, insert key 2, verify we can continue iteration
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.find_u64(1);
            cursor.insert_u64(2, b"two".to_vec());
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
        // Insert enough to trigger page split, verify all keys still visited
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1-50
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=50u64 {
                cursor.insert_u64(i, format!("value_{}", i).into_bytes());
            }
        }

        // Position at key 10, insert more keys to force split, verify iteration
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.find_u64(10);

            // Insert 50 more keys to force splits
            for i in 51..=100u64 {
                cursor.insert_u64(i, format!("value_{}", i).into_bytes());
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1-10
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=10u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Delete all keys forward
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.first();

            while cursor.get_entry().is_some() {
                cursor.delete_current();
                // After delete_current, cursor is in RequiresSeek state
                // next() will reposition to the next key (successor of deleted key)
                cursor.next();
            }
        }

        // Verify tree is empty
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();
            assert!(cursor.get_entry().is_none());
        }
    }

    #[test]
    fn test_cursor_get_entry_after_mutation() {
        // Verify get_entry() works after insert/delete
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Insert keys 1, 2, 3
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for i in 1..=3u64 {
                cursor.insert_u64(i, i.to_be_bytes().to_vec());
            }
        }

        // Position at key 2, delete it, verify get_entry() still works
        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.find_u64(2);
            cursor.delete_current();

            // get_entry() should trigger ensure_positioned() and find key 3
            let entry = cursor.get_entry().unwrap();
            assert_eq!(entry.key(), encode_u64_key(3).as_slice());
        }
    }

    #[test]
    fn test_cursor_refind_after_insert() {
        // Verify that after inserts, we can position and navigate correctly
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        {
            use super::encode_u64_key;
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();

            // Insert multiple values
            cursor.insert_u64(1, b"value1".to_vec());
            cursor.insert_u64(2, b"value2".to_vec());
            cursor.insert_u64(3, b"value3".to_vec());

            // Position using first()
            cursor.first();

            // Should be positioned on first entry
            let entry = cursor.get_entry().expect("Should find first entry");
            assert_eq!(entry.key(), encode_u64_key(1).as_slice());

            // Navigate to next
            cursor.next();
            let entry = cursor.get_entry().expect("Should find second entry");
            assert_eq!(entry.key(), encode_u64_key(2).as_slice());

            // Navigate to next again
            cursor.next();
            let entry = cursor.get_entry().expect("Should find third entry");
            assert_eq!(entry.key(), encode_u64_key(3).as_slice());
        }
    }

    // ========================================================================
    // Variable-length key tests (Step 41.7)
    // ========================================================================

    #[test]
    fn test_variable_length_keys_ordering() {
        // Keys of different lengths should sort lexicographically:
        // "a" < "ab" < "abc" < "b"
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let keys: Vec<&[u8]> = vec![b"abc", b"b", b"a", b"ab"];
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for key in &keys {
                cursor.insert(key, b"value".to_vec());
            }
        }

        // Scan and verify sorted order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let expected_order: Vec<&[u8]> = vec![b"a", b"ab", b"abc", b"b"];
            for expected_key in expected_order {
                let entry = cursor.get_entry().expect("Should have entry");
                assert_eq!(entry.key(), expected_key);
                cursor.next();
            }
            assert!(cursor.get_entry().is_none(), "Should be at end");
        }
    }

    #[test]
    fn test_variable_length_keys_long_key() {
        // Keys up to 1KB should work correctly
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let long_key: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let short_key: Vec<u8> = b"short".to_vec();
        let medium_key: Vec<u8> = b"medium_length_key_here".to_vec();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert(&long_key, b"long_value".to_vec());
            cursor.insert(&short_key, b"short_value".to_vec());
            cursor.insert(&medium_key, b"medium_value".to_vec());
        }

        // Verify long key can be found and read back
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find(&long_key);
            assert!(found, "Long key should be found");
            let entry = cursor.get_entry().unwrap();
            assert_eq!(entry.key(), long_key.as_slice());
        }

        // Verify short key can be found
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            let found = cursor.find(&short_key);
            assert!(found, "Short key should be found");
        }
    }

    #[test]
    fn test_variable_length_keys_mixed_lengths() {
        // Mix of 1-byte, 4-byte, 8-byte, and 16-byte keys should all sort correctly
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
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
            let mut cursor = cursor_handle.open_readwrite();
            for key in &keys {
                cursor.insert(key, b"v".to_vec());
            }
        }

        // Scan and verify they come back in sorted order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
            cursor.first();

            let mut prev_key: Vec<u8> = vec![];
            let mut count = 0;
            while let Some(entry) = cursor.get_entry() {
                let k = entry.key().to_vec();
                assert!(
                    k > prev_key,
                    "Keys must be strictly increasing: {:?} > {:?}",
                    k,
                    prev_key
                );
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
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        // Generate variable-length keys of varying sizes
        let mut keys: Vec<Vec<u8>> = (0u32..200)
            .map(|i| {
                let len = 1 + (i as usize % 20); // lengths 1..20
                let mut k = vec![0u8; len];
                k[0] = (i >> 8) as u8;
                if len > 1 {
                    k[1] = (i & 0xFF) as u8;
                }
                k
            })
            .collect();

        // Deduplicate and sort to get the canonical set
        keys.sort();
        keys.dedup();
        let num_keys = keys.len();

        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            for key in &keys {
                cursor.insert(key, b"val".to_vec());
            }
            cursor.verify().unwrap();
        }

        // Scan and verify all keys present in sorted order
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readonly();
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

    /// Return the number of pages currently allocated in the btree's pager.
    fn page_count(btree: &BTree) -> u32 {
        btree.pager.borrow().get_file_size_pages()
    }

    /// Insert a value and return the number of overflow pages allocated (pages_after - pages_before - 1).
    /// The -1 accounts for the leaf page that always exists.
    fn overflow_pages_for(btree: &mut BTree, root: u32, key: u64, value: Vec<u8>) -> u32 {
        let before = page_count(btree);
        {
            let mut cursor_handle = btree.open(root);
            let mut cursor = cursor_handle.open_readwrite();
            cursor.insert_u64(key, value);
        }
        let after = page_count(btree);
        // Subtract 1 for any potential leaf split page; use saturating sub to avoid underflow
        after.saturating_sub(before)
    }

    #[test]
    fn test_chunk_threshold_no_overflow() {
        // A value of exactly CHUNK_THRESHOLD bytes should store inline — no overflow pages.
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let value = vec![0xAAu8; CHUNK_THRESHOLD];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 0,
            "CHUNK_THRESHOLD bytes should store inline (no overflow pages), but got {pages_added} new pages"
        );

        // Verify round-trip
        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value);
    }

    #[test]
    fn test_chunk_threshold_plus_one_spills_to_one_overflow_page() {
        // A value of CHUNK_THRESHOLD + 1 bytes should spill to exactly one overflow page.
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let value = vec![0x55u8; CHUNK_THRESHOLD + 1];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 1,
            "CHUNK_THRESHOLD+1 bytes should use exactly 1 overflow page, but got {pages_added} new pages"
        );

        // Verify round-trip
        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value);
    }

    #[test]
    fn test_overflow_limit_boundary_two_pages() {
        // A value of CHUNK_THRESHOLD + OVERFLOW_LIMIT bytes:
        //   - CHUNK_THRESHOLD bytes inline
        //   - OVERFLOW_LIMIT bytes in exactly one overflow page
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let value = vec![0x77u8; CHUNK_THRESHOLD + OVERFLOW_LIMIT];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 1,
            "CHUNK_THRESHOLD + OVERFLOW_LIMIT bytes should fit in exactly 1 overflow page, got {pages_added}"
        );

        // Verify round-trip
        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value);
    }

    #[test]
    fn test_overflow_chain_three_pages() {
        // A value of CHUNK_THRESHOLD + OVERFLOW_LIMIT * 2 + 1 bytes:
        //   - CHUNK_THRESHOLD bytes inline
        //   - OVERFLOW_LIMIT bytes in page 1
        //   - OVERFLOW_LIMIT bytes in page 2
        //   - 1 byte in page 3
        // Total: 3 overflow pages
        let test = TestDb::default();
        let mut btree: BTree = test.catalog.into();
        let root = btree.create_tree();

        let value = vec![0x33u8; CHUNK_THRESHOLD + OVERFLOW_LIMIT * 2 + 1];
        let pages_added = overflow_pages_for(&mut btree, root, 1, value.clone());
        assert_eq!(
            pages_added, 3,
            "CHUNK_THRESHOLD + OVERFLOW_LIMIT*2 + 1 bytes should create 3 overflow pages, got {pages_added}"
        );

        // Verify round-trip
        let mut cursor_handle = btree.open(root);
        let mut cursor = cursor_handle.open_readonly();
        cursor.first();
        let mut buf = Vec::new();
        cursor.get_entry().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value);
    }
}
