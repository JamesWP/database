//! Prototype API design for Pager / NodePageStore / BTree.
//!
//! All method bodies are `todo!()` stubs — this file exists to verify that the
//! ownership and borrow structure compiles correctly, particularly for the write
//! paths where we want zero NodePage clones.
#![allow(dead_code, unused_variables, unused_mut, unused_imports, unreachable_code)]

use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Constants
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub const PAGE_SIZE: usize = 4096;
const CHUNK_THRESHOLD: usize = 1007;
const OVERFLOW_LIMIT: usize = 4052;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PageId — newtype wrapping u32
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Error
//
// PageFull carries the node back to the caller so it can split without
// needing to re-fetch. This is what makes the write path clone-free.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    /// The node did not fit on a single page. Returns ownership so the caller
    /// can split the node without an extra fetch.
    PageFull(NodePage),
    Decode(String),
    FormatError(String),
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// NodePage stubs — enough structure for borrow checking
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone, Debug)]
pub struct LeafNodePage {
    cells: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct InteriorNodePage {
    _children: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct OverflowPage {
    content: Vec<u8>,
    continuation: Option<PageId>,
}

#[derive(Clone, Debug)]
pub enum NodePage {
    Leaf(LeafNodePage),
    Interior(InteriorNodePage),
    Overflow(OverflowPage),
}

pub enum SearchResult {
    Found(usize),
    NotPresent(usize),
    GoDown(usize, PageId),
}

impl NodePage {
    pub fn empty_leaf() -> Self {
        NodePage::Leaf(LeafNodePage { cells: vec![] })
    }

    pub fn search(&self, _key: &[u8]) -> SearchResult {
        todo!()
    }

    /// Consume self and return (left_half, right_half).
    pub fn split(self) -> (NodePage, NodePage) {
        todo!()
    }

    pub fn smallest_key(&self) -> Vec<u8> {
        todo!()
    }

    pub fn interior_mut(&mut self) -> Option<&mut InteriorNodePage> {
        match self {
            NodePage::Interior(i) => Some(i),
            _ => None,
        }
    }
}

impl InteriorNodePage {
    pub fn new(_left: PageId, _separator: Vec<u8>, _right: PageId) -> Self {
        InteriorNodePage { _children: vec![] }
    }

    pub fn insert_child_page(&mut self, _key: Vec<u8>, _id: PageId) {
        todo!()
    }
}

impl LeafNodePage {
    pub fn insert_at(&mut self, _idx: usize, _key: Vec<u8>, _value: Vec<u8>) {
        todo!()
    }

    pub fn remove_at(&mut self, _idx: usize) {
        todo!()
    }

    pub fn get_key(&self, _idx: usize) -> Vec<u8> {
        todo!()
    }
}

impl OverflowPage {
    pub fn new(content: Vec<u8>, continuation: Option<PageId>) -> Self {
        OverflowPage { content, continuation }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Pager — raw byte I/O
//
// Intentionally unexported. All access goes through NodePageStore.
// Owns the free list and format metadata internally.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

struct Pager {}

impl Pager {
    fn open(_path: &Path) -> Result<Self, Error> {
        Ok(Pager {})
    }

    fn page_count(&self) -> u32 {
        todo!()
    }

    fn allocate(&mut self) -> Result<PageId, Error> {
        todo!()
    }

    fn free(&mut self, _id: PageId) -> Result<(), Error> {
        todo!()
    }

    fn read_raw(&self, _id: PageId) -> Result<[u8; PAGE_SIZE], Error> {
        todo!()
    }

    fn write_raw(&mut self, _id: PageId, _bytes: &[u8; PAGE_SIZE]) -> Result<(), Error> {
        todo!()
    }

    fn flush(&mut self) -> Result<(), Error> {
        todo!()
    }

    fn validate_format_version(&self) -> Result<(), Error> {
        todo!()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Encoding helpers
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Returns None if the encoded form exceeds PAGE_SIZE.
fn encode_node(_node: &NodePage) -> Option<[u8; PAGE_SIZE]> {
    todo!()
}

fn decode_node(_raw: &[u8; PAGE_SIZE]) -> Result<NodePage, Error> {
    todo!()
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// NodePageStore — cache + CBOR layer over Pager
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct NodePageStore {
    pager: Pager,
    cache: HashMap<PageId, NodePage>,
}

impl NodePageStore {
    pub fn open(path: &Path) -> Result<Self, Error> {
        Ok(NodePageStore {
            pager: Pager::open(path)?,
            cache: HashMap::new(),
        })
    }

    pub fn page_count(&self) -> u32 {
        self.pager.page_count()
    }

    pub fn validate_format_version(&self) -> Result<(), Error> {
        self.pager.validate_format_version()
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.pager.flush()
    }

    pub fn allocate(&mut self) -> Result<PageId, Error> {
        self.pager.allocate()
    }

    pub fn free(&mut self, id: PageId) -> Result<(), Error> {
        self.cache.remove(&id);
        self.pager.free(id)
    }

    /// Read a node, populating the cache on a miss.
    ///
    /// Takes `&mut self` so cache population doesn't require `RefCell`.
    /// Callers that need to hold the reference across an `allocate` or `write`
    /// must use `take` instead.
    pub fn read(&mut self, id: PageId) -> Result<&NodePage, Error> {
        // Split into two calls so the borrow checker can see that the &mut borrow
        // in ensure_cached ends before the shared borrow in cache.get().
        self.ensure_cached(id)?;
        Ok(self.cache.get(&id).unwrap())
    }

    fn ensure_cached(&mut self, id: PageId) -> Result<(), Error> {
        if !self.cache.contains_key(&id) {
            let raw = self.pager.read_raw(id)?;
            let node = decode_node(&raw)?;
            self.cache.insert(id, node);
        }
        Ok(())
    }

    /// Remove a node from the cache and return it owned.
    ///
    /// On a cache miss, fetches from disk without inserting. The caller is
    /// expected to mutate the node and pass it back to `write`. Because no
    /// borrow is held, `allocate` can be called freely between `take` and `write`.
    pub fn take(&mut self, id: PageId) -> Result<NodePage, Error> {
        if let Some(node) = self.cache.remove(&id) {
            return Ok(node);
        }
        let raw = self.pager.read_raw(id)?;
        decode_node(&raw)
    }

    /// Write a node, consuming it.
    ///
    /// On success: encodes to disk and inserts into cache. No clone at either site.
    ///
    /// On `PageFull`: returns `Err(Error::PageFull(node))`, giving ownership back
    /// to the caller so it can split the node without a re-fetch or clone.
    pub fn write(&mut self, id: PageId, node: NodePage) -> Result<(), Error> {
        match encode_node(&node) {
            None => Err(Error::PageFull(node)),
            Some(bytes) => {
                self.pager.write_raw(id, &bytes)?;
                self.cache.insert(id, node);
                Ok(())
            }
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BTree
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone)]
pub struct BTree {
    store: Arc<RefCell<NodePageStore>>,
}

impl BTree {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let store = NodePageStore::open(path)?;
        let btree = BTree {
            store: Arc::new(RefCell::new(store)),
        };
        {
            let s = btree.store.borrow();
            if s.page_count() > 0 {
                s.validate_format_version()?;
            }
        }
        Ok(btree)
    }

    /// Allocate a root page with an empty leaf. Returns the root PageId.
    pub fn create_tree(&self) -> Result<PageId, Error> {
        let mut store = self.store.borrow_mut();
        let root_id = store.allocate()?;
        store.write(root_id, NodePage::empty_leaf())?;
        Ok(root_id)
    }

    pub fn cursor(&self, root: PageId) -> CursorHandle {
        CursorHandle { store: Arc::clone(&self.store), root }
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.store.borrow_mut().flush()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CursorHandle + Cursor
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct CursorHandle {
    store: Arc<RefCell<NodePageStore>>,
    root: PageId,
}

/// An active cursor that holds exclusive access to the store for its lifetime.
pub struct Cursor<'a> {
    store: RefMut<'a, NodePageStore>,
    root: PageId,
}

impl CursorHandle {
    pub fn open(&mut self) -> Cursor<'_> {
        Cursor { store: self.store.borrow_mut(), root: self.root }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Cursor — read-write operations
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

impl<'a> Cursor<'a> {
    // ── Insert ─────────────────────────────────────────────────────────────

    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        // If the value is too large for one page, spill the tail into overflow
        // pages first. This requires allocating before we touch any node pages,
        // which is fine because we hold no borrows yet.
        let continuation = if value.len() > CHUNK_THRESHOLD {
            let overflow_id = self.write_overflow_chain(&value[CHUNK_THRESHOLD..])?;
            Some(overflow_id)
        } else {
            None
        };

        // Walk from root to the insertion leaf, following interior nodes.
        // `read` is called in a block so its borrow drops before `take`.
        let mut stack = vec![self.root];
        loop {
            let top_id = *stack.last().unwrap();

            // Borrow just long enough to get the search result (which is Copy/owned).
            let result = {
                let node = self.store.read(top_id)?;
                node.search(key)
            };

            match result {
                SearchResult::Found(idx) | SearchResult::NotPresent(idx) => {
                    // take() removes from cache; borrow checker is happy because
                    // there is no outstanding &NodePage from read() at this point.
                    let mut node = self.store.take(top_id)?;
                    match &mut node {
                        NodePage::Leaf(leaf) => {
                            leaf.insert_at(idx, key.to_vec(), value);
                        }
                        _ => unreachable!("insertion point must be a leaf"),
                    }
                    return self.update_page(node, stack);
                }
                SearchResult::GoDown(_, child_id) => {
                    stack.push(child_id);
                }
            }
        }
    }

    /// Write `rest` as a linked chain of overflow pages.
    /// All nodes are constructed fresh — no clone needed.
    fn write_overflow_chain(&mut self, mut rest: &[u8]) -> Result<PageId, Error> {
        // Allocate the first page before entering the loop.
        let mut page_id = self.store.allocate()?;
        let first_id = page_id;

        while rest.len() > OVERFLOW_LIMIT {
            let next_id = self.store.allocate()?;
            let chunk = rest[..OVERFLOW_LIMIT].to_vec();
            self.store.write(
                page_id,
                NodePage::Overflow(OverflowPage::new(chunk, Some(next_id))),
            )?;
            rest = &rest[OVERFLOW_LIMIT..];
            page_id = next_id;
        }

        self.store.write(
            page_id,
            NodePage::Overflow(OverflowPage::new(rest.to_vec(), None)),
        )?;

        Ok(first_id)
    }

    // ── Delete ─────────────────────────────────────────────────────────────

    /// Delete the cell at `(leaf_id, cell_index)`. Returns the deleted key.
    ///
    /// Uses `take` so the page is loaded once and mutated in place — one load,
    /// no NodePage clone, compared to the two `get_and_decode_node` calls in
    /// the current implementation.
    pub fn delete_at(&mut self, leaf_id: PageId, cell_index: usize) -> Result<Vec<u8>, Error> {
        let mut node = self.store.take(leaf_id)?;

        let deleted_key = match &node {
            NodePage::Leaf(leaf) => leaf.get_key(cell_index),
            _ => panic!("cursor position must be a leaf"),
        };
        match &mut node {
            NodePage::Leaf(leaf) => leaf.remove_at(cell_index),
            _ => unreachable!(),
        }

        // Deletion cannot overflow a page; if it does something has gone wrong.
        self.store.write(leaf_id, node)?;
        Ok(deleted_key)
    }

    // ── Traversal ──────────────────────────────────────────────────────────

    /// Walk from root to the leaf holding `key`. Returns `(leaf_id, index)`.
    ///
    /// `read` is used (not `take`) because traversal is read-only. Each borrow
    /// is scoped to extract the `SearchResult`, then dropped before the next read.
    pub fn find(&mut self, key: &[u8]) -> Result<Option<(PageId, usize)>, Error> {
        let mut page_id = self.root;
        loop {
            // SearchResult contains only PageId (Copy) so it's owned after the block.
            let result = {
                let node = self.store.read(page_id)?;
                node.search(key)
            };
            match result {
                SearchResult::Found(idx) => return Ok(Some((page_id, idx))),
                SearchResult::NotPresent(_) => return Ok(None),
                SearchResult::GoDown(_, child_id) => page_id = child_id,
            }
        }
    }

    // ── Internal write helpers ─────────────────────────────────────────────

    /// Write a (possibly modified) node. On `PageFull`, split and retry.
    fn update_page(&mut self, node: NodePage, stack: Vec<PageId>) -> Result<(), Error> {
        let id = *stack.last().unwrap();
        match self.store.write(id, node) {
            Ok(()) => Ok(()),
            // write() returns ownership on PageFull — no re-fetch, no clone.
            Err(Error::PageFull(node)) => self.split_page(node, stack),
            Err(e) => Err(e),
        }
    }

    /// Split an overfull node and relink the tree.
    ///
    /// Key invariant: all `allocate` calls happen before any borrows from
    /// `read` or `take`, so they never conflict.
    fn split_page(&mut self, overfull: NodePage, mut stack: Vec<PageId>) -> Result<(), Error> {
        let overfull_id = stack.pop().unwrap();
        let (left_half, right_half) = overfull.split();
        let right_first_key = right_half.smallest_key();

        // Allocate before any writes (no borrows outstanding at this point).
        let right_id = self.store.allocate()?;

        if !stack.is_empty() {
            // ── Non-root split ────────────────────────────────────────
            // Left half stays at overfull_id; right half goes to right_id.
            self.store.write(overfull_id, left_half)?;
            self.store.write(right_id, right_half)?;

            // Fix up the parent: take it out, insert the new separator, write back.
            // take() + write() — no clone.
            let parent_id = stack.pop().unwrap();
            let mut parent = self.store.take(parent_id)?;
            parent
                .interior_mut()
                .expect("parent of a split must be an interior node")
                .insert_child_page(right_first_key, right_id);

            match self.store.write(parent_id, parent) {
                Ok(()) => Ok(()),
                // Parent overflowed too. Ownership is returned; recurse.
                Err(Error::PageFull(parent)) => {
                    stack.push(parent_id);
                    self.split_page(parent, stack)
                }
                Err(e) => Err(e),
            }
        } else {
            // ── Root split ────────────────────────────────────────────
            // The root page index must remain stable because external state
            // holds it as the tree's entry point.
            //
            // Both halves move to fresh pages. The root is overwritten with a
            // new interior node. Crucially: we never write left_half to
            // overfull_id, so there is no intermediate write + re-read.
            let left_id = self.store.allocate()?;

            self.store.write(left_id, left_half)?;   // move — no clone
            self.store.write(right_id, right_half)?;  // move — no clone

            let interior = NodePage::Interior(
                InteriorNodePage::new(left_id, right_first_key, right_id)
            );
            self.store.write(overfull_id, interior)?; // move — no clone
            Ok(())
        }
    }
}

fn main() {
    println!("pager-design: API stub compiles.");
}
