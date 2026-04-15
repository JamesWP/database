use std::collections::{HashMap, HashSet};
use std::path::Path;

use probe::probe;

use super::error::Error;
use super::node::NodePage;
use super::page_id::PageId;
use super::pager::{Pager, PAGE_SIZE};

/// The middle layer of the storage stack.
///
/// Wraps `Pager` (raw file I/O) and owns a decoded `NodePage` cache.
/// All B-tree code talks exclusively to `NodePageStore` — it never touches
/// `Pager` directly.
///
/// ## Ownership model
///
/// * `read(id)` — borrows a `&NodePage` from the cache (populating on miss).
///   The borrow must be dropped before calling `allocate`, `take`, or `write`.
/// * `take(id)` — removes the entry from the cache and dirty set, returning
///   the `NodePage` owned.  The caller mutates it and hands it back with `write`.
/// * `write(id, node)` — encodes the node (to check for `PageFull`) and marks
///   it dirty; the encoded bytes are **not** written to disk until `flush`.
///   On overflow returns `Err(Error::PageFull(node))` so the caller gets the
///   node back for splitting.
/// * `flush()` — writes all dirty pages to disk in one pass, then flushes the
///   file handle.  Also called automatically on drop.
/// * `allocate` — must be called **before** any `read`/`take` for the same
///   operation; the borrow checker enforces this.
#[derive(Debug)]
pub struct NodePageStore {
    pager: Pager,
    cache: HashMap<PageId, NodePage>,
    /// IDs of pages that have been written since the last flush.
    /// Their decoded nodes live in `cache`.  Encoded and written to disk on `flush()`.
    dirty: HashSet<PageId>,
}

impl NodePageStore {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let path_str = path
            .to_str()
            .ok_or_else(|| Error::Io(std::io::Error::other("non-UTF-8 path")))?;
        Ok(NodePageStore {
            pager: Pager::new(path_str),
            cache: HashMap::new(),
            dirty: HashSet::new(),
        })
    }

    pub fn page_count(&self) -> u32 {
        self.pager.get_file_size_pages()
    }

    pub fn validate_format_version(&self) -> Result<(), Error> {
        match self.format_version() {
            None => Ok(()), // empty file — no version to validate
            Some(0) => Err(Error::FormatError(
                "Database format version 0 (JSON) is no longer supported. \
                 Please recreate your database."
                    .into(),
            )),
            Some(1) => Err(Error::FormatError(
                "Database format version 1 is no longer supported. \
                 Please recreate your database."
                    .into(),
            )),
            Some(2) => Err(Error::FormatError(
                "Database format version 2 is no longer supported. \
                 Please recreate your database."
                    .into(),
            )),
            Some(3) => Ok(()),
            Some(v) => Err(Error::FormatError(format!(
                "Unknown database format version {}. \
                 This database may have been created by a newer version.",
                v
            ))),
        }
    }

    /// Allocate a fresh page and return its `PageId`.
    /// Must be called before any `read` or `take` within the same operation.
    pub fn allocate(&mut self) -> Result<PageId, Error> {
        Ok(self.pager.allocate())
    }

    /// Return a page to the free list and evict it from the cache and dirty set.
    pub fn free(&mut self, id: PageId) -> Result<(), Error> {
        self.cache.remove(&id);
        self.dirty.remove(&id);
        self.pager.free(id);
        Ok(())
    }


    /// Borrow a `&NodePage` from the cache (fetching from disk on miss).
    ///
    /// The returned reference must be dropped before calling `allocate`,
    /// `take`, or `write` — the borrow checker enforces this.
    pub fn read(&mut self, id: PageId) -> Result<&NodePage, Error> {
        self.ensure_cached(id)?;
        Ok(self.cache.get(&id).unwrap())
    }

    /// Remove the page from the cache and dirty set, returning it owned.
    ///
    /// On a cache miss the page is loaded from disk without being inserted
    /// into the cache.  The caller is expected to hand it back via `write`.
    pub fn take(&mut self, id: PageId) -> Result<NodePage, Error> {
        self.dirty.remove(&id);
        if let Some(node) = self.cache.remove(&id) {
            return Ok(node);
        }
        // Cache miss: decode directly from disk.
        let bytes = self.pager.read_raw(id);
        decode_page(&bytes)
    }

    /// Mark `node` as dirty — it will be encoded and written to disk on the next
    /// `flush()`.  No CBOR encoding or disk I/O happens here in the common case.
    ///
    /// When `node.cbor_size_estimate()` exceeds `PAGE_SIZE` the node is encoded
    /// immediately to confirm whether it truly overflows.  If it does, returns
    /// `Err(Error::PageFull(node))` so the caller can split — otherwise the node
    /// is stored in the dirty set as normal (the confirmed-fit encoded bytes are
    /// discarded; the node will be re-encoded at flush).
    pub fn write(&mut self, id: PageId, node: NodePage) -> Result<(), Error> {
        let node = if node.cbor_size_estimate() > PAGE_SIZE as usize {
            // Estimate says the page might overflow — encode to confirm.
            let (_bytes, node) = encode_page(node)?; // may return Err(PageFull(node))
            node
        } else {
            node
        };
        self.cache.insert(id, node);
        self.dirty.insert(id);
        Ok(())
    }

    /// Encode all dirty pages, write them to disk, and flush the file handle.
    ///
    /// Called explicitly after each query and automatically on drop.
    pub fn flush(&mut self) -> Result<(), Error> {
        let ids: Vec<PageId> = self.dirty.drain().collect();
        for id in ids {
            let node = self.cache.remove(&id).expect("dirty page must be in cache");
            let (bytes, node) = match encode_page(node) {
                Ok(r) => r,
                Err(Error::PageFull(_node)) => {
                    panic!(
                        "NodePageStore: page {:?} overflowed at flush — \
                         cbor_size_estimate() missed this case",
                        id
                    );
                }
                Err(e) => return Err(e),
            };
            self.pager.write_raw(id, &bytes);
            self.cache.insert(id, node);
        }
        self.pager.flush()?;
        Ok(())
    }

    /// Return the on-disk format version, or `None` if the database is empty.
    pub fn format_version(&self) -> Option<u16> {
        self.pager.get_zero_page().map(|z| z.format_version)
    }

    /// Return a debug string representation of the ZeroPage for diagnostic
    /// tools (e.g. `BTree::inspect_page`).  Returns `None` if the database is
    /// empty.
    pub fn zero_page_debug(&self) -> Option<String> {
        self.pager.get_zero_page().map(|z| format!("{:#?}", z))
    }

    /// Visit every `(leaf_page_idx, cell_idx)` pair in the tree rooted at
    /// `root` in key order, calling `f` for each.
    ///
    /// Interior nodes are traversed recursively; overflow pages are skipped
    /// (they are read transparently by [`CellReader`]).  Read errors on any
    /// page silently stop traversal of that subtree — the caller should run
    /// `verify` separately if it needs to detect corruption.
    ///
    /// [`CellReader`]: super::cell_reader::CellReader
    pub fn scan_leaf_cells(&mut self, root: PageId, f: &mut impl FnMut(&mut Self, u32, usize)) {
        // Clone the page so we release the borrow before recursing or calling f.
        let page = match self.read(root) {
            Ok(p) => p.clone(),
            Err(_) => return,
        };
        match page {
            NodePage::Leaf(ref leaf) => {
                let n = leaf.num_items();
                for i in 0..n {
                    f(self, root.as_u32(), i);
                }
            }
            NodePage::Interior(interior) => {
                let edges: Vec<u32> = (0..interior.num_edges())
                    .map(|i| interior.get_child_page_by_index(i))
                    .collect();
                for child in edges {
                    self.scan_leaf_cells(PageId(child), f);
                }
            }
            NodePage::OverflowPage(_) => {}
        }
    }

    // ── private helpers ─────────────────────────────────────────────────────

    fn ensure_cached(&mut self, id: PageId) -> Result<(), Error> {
        if self.cache.contains_key(&id) {
            probe!(database, page_read_cache_hit, id.as_u32());
            return Ok(());
        }
        let bytes = self.pager.read_raw(id);
        let node = decode_page(&bytes)?;
        match &node {
            NodePage::Leaf(_) => probe!(database, page_read_leaf, id.as_u32()),
            NodePage::Interior(_) => probe!(database, page_read_interior, id.as_u32()),
            NodePage::OverflowPage(_) => probe!(database, page_read_overflow, id.as_u32()),
        }
        self.cache.insert(id, node);
        Ok(())
    }
}

impl Drop for NodePageStore {
    fn drop(&mut self) {
        self.flush().expect("NodePageStore: flush on drop failed");
    }
}

// ── CBOR page helpers ─────────────────────────────────────────────────────────

fn decode_page(bytes: &[u8]) -> Result<NodePage, Error> {
    probe!(database, cbor_page_decode);
    ciborium::de::from_reader(bytes).map_err(|e| Error::Decode(e.to_string()))
}

/// Encode `node` into a fixed-size page buffer.
///
/// Returns `Err(Error::PageFull(node))` when the encoded size exceeds
/// `PAGE_SIZE` — the node is returned to the caller so a split can be
/// performed without a re-fetch or clone.
fn encode_page(node: NodePage) -> Result<([u8; PAGE_SIZE as usize], NodePage), Error> {
    let mut bytes = [0u8; PAGE_SIZE as usize];
    probe!(database, cbor_page_encode);
    match ciborium::ser::into_writer(&node, &mut &mut bytes[..]) {
        Ok(()) => Ok((bytes, node)),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("failed to write whole buffer") || msg.contains("write zero") {
                Err(Error::PageFull(node))
            } else {
                Err(Error::Decode(format!("CBOR encoding failed: {}", msg)))
            }
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::storage::node::{LeafNodePage, NodePage};

    fn open_store() -> (NamedTempFile, NodePageStore) {
        let file = NamedTempFile::new().unwrap();
        let store = NodePageStore::open(file.path()).unwrap();
        (file, store)
    }

    fn leaf() -> NodePage {
        NodePage::Leaf(LeafNodePage::default())
    }

    #[test]
    fn read_populates_cache_on_miss() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();
        // Flush to disk, then clear both cache and dirty set to simulate a cold read.
        store.flush().unwrap();
        store.cache.clear();
        store.dirty.clear();

        assert!(!store.cache.contains_key(&id), "cache should be empty");
        let _ = store.read(id).unwrap();
        assert!(store.cache.contains_key(&id), "read should populate cache");
    }

    #[test]
    fn second_read_is_cache_hit() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();
        store.flush().unwrap();
        store.cache.clear();
        store.dirty.clear();

        let _ = store.read(id).unwrap();
        let len_after_first = store.cache.len();
        let _ = store.read(id).unwrap();
        assert_eq!(
            store.cache.len(),
            len_after_first,
            "cache size unchanged on second read"
        );
    }

    #[test]
    fn take_removes_from_cache() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();

        assert!(store.cache.contains_key(&id));
        let _ = store.take(id).unwrap();
        assert!(
            !store.cache.contains_key(&id),
            "take must evict cache entry"
        );
    }

    #[test]
    fn write_after_take_repopulates_cache() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();

        let node = store.take(id).unwrap();
        assert!(!store.cache.contains_key(&id));
        store.write(id, node).unwrap();
        assert!(
            store.cache.contains_key(&id),
            "write should repopulate cache"
        );
    }

    #[test]
    fn take_on_miss_loads_from_disk() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();
        store.flush().unwrap();
        store.cache.clear();
        store.dirty.clear();

        let node = store.take(id).unwrap();
        assert!(matches!(node, NodePage::Leaf(_)));
        // Cache remains empty after take-on-miss.
        assert!(!store.cache.contains_key(&id));
    }

    #[test]
    fn free_evicts_cache_entry() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();

        assert!(store.cache.contains_key(&id));
        store.free(id).unwrap();
        assert!(
            !store.cache.contains_key(&id),
            "free must evict cache entry"
        );
    }

    #[test]
    fn write_is_deferred_until_flush() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();

        // Page is dirty but not yet on disk — cold read from disk must fail.
        assert!(store.dirty.contains(&id), "page should be dirty after write");
        let bytes = store.pager.read_raw(id);
        let disk_result = decode_page(&bytes);
        assert!(
            disk_result.is_err(),
            "page should not be readable from disk before flush"
        );

        // After flush the page is on disk and the dirty set is cleared.
        store.flush().unwrap();
        assert!(!store.dirty.contains(&id), "dirty set should be empty after flush");
        let bytes = store.pager.read_raw(id);
        assert!(decode_page(&bytes).is_ok(), "page should be readable from disk after flush");
    }

    #[test]
    fn take_clears_dirty_entry() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();
        assert!(store.dirty.contains(&id));

        store.take(id).unwrap();
        assert!(!store.dirty.contains(&id), "take must clear dirty entry");
    }

    #[test]
    fn repeated_writes_produce_one_disk_write_per_flush() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();

        // Write the same page three times — dirty map should only hold one entry.
        store.write(id, leaf()).unwrap();
        store.take(id).unwrap();
        store.write(id, leaf()).unwrap();
        store.take(id).unwrap();
        store.write(id, leaf()).unwrap();

        assert_eq!(store.dirty.len(), 1, "multiple writes to same page collapse to one dirty entry");
    }
}
