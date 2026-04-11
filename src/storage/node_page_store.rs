use std::collections::HashMap;
use std::path::Path;

use probe::probe;

use super::node::NodePage;
use super::error::Error;
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
/// * `take(id)` — removes the entry from the cache and returns the `NodePage`
///   owned.  The caller mutates it and hands it back with `write`.
/// * `write(id, node)` — encodes the node to disk and stores it in the cache
///   (no clone).  On overflow returns `Err(Error::PageFull(node))` so the
///   caller gets the node back for splitting.
/// * `allocate` — must be called **before** any `read`/`take` for the same
///   operation; the borrow checker enforces this.
#[derive(Debug)]
pub struct NodePageStore {
    pager: Pager,
    cache: HashMap<PageId, NodePage>,
}

impl NodePageStore {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let path_str = path
            .to_str()
            .ok_or_else(|| Error::Io(std::io::Error::other("non-UTF-8 path")))?;
        Ok(NodePageStore {
            pager: Pager::new(path_str),
            cache: HashMap::new(),
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
            Some(2) => Ok(()),
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

    /// Return a page to the free list and evict it from the cache.
    pub fn free(&mut self, id: PageId) -> Result<(), Error> {
        self.cache.remove(&id);
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

    /// Remove the page from the cache and return it owned.
    ///
    /// On a cache miss the page is loaded from disk without being inserted
    /// into the cache.  The caller is expected to hand it back via `write`.
    pub fn take(&mut self, id: PageId) -> Result<NodePage, Error> {
        if let Some(node) = self.cache.remove(&id) {
            return Ok(node);
        }
        // Cache miss: decode directly from disk.
        let bytes = self.pager.read_raw(id);
        probe!(database, cbor_page_decode);
        ciborium::de::from_reader(&bytes[..]).map_err(|e| Error::Decode(e.to_string()))
    }

    /// Encode `node` and write it to disk, inserting it into the cache.
    ///
    /// Returns `Err(Error::PageFull(node))` when the encoded representation
    /// exceeds `PAGE_SIZE` — the node is returned to the caller so a split
    /// can be performed without a re-fetch or clone.
    pub fn write(&mut self, id: PageId, node: NodePage) -> Result<(), Error> {
        let mut bytes = [0u8; PAGE_SIZE as usize];
        probe!(database, cbor_page_encode);
        match ciborium::ser::into_writer(&node, &mut &mut bytes[..]) {
            Ok(()) => {
                self.pager.write_raw(id, &bytes);
                self.cache.insert(id, node);
                Ok(())
            }
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

    /// Flush the underlying file handle.
    pub fn flush(&mut self) -> Result<(), Error> {
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

    // ── private helpers ──────────────────────────────────────────────────────

    fn ensure_cached(&mut self, id: PageId) -> Result<(), Error> {
        if self.cache.contains_key(&id) {
            return Ok(());
        }
        let bytes = self.pager.read_raw(id);
        probe!(database, cbor_page_decode);
        let node: NodePage =
            ciborium::de::from_reader(&bytes[..]).map_err(|e| Error::Decode(e.to_string()))?;
        match &node {
            NodePage::Leaf(_) => probe!(database, page_read_leaf, id.as_u32()),
            NodePage::Interior(_) => probe!(database, page_read_interior, id.as_u32()),
            NodePage::OverflowPage(_) => probe!(database, page_read_overflow, id.as_u32()),
        }
        self.cache.insert(id, node);
        Ok(())
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
        // Clear cache to simulate a cold read.
        store.cache.clear();

        assert!(!store.cache.contains_key(&id), "cache should be empty");
        let _ = store.read(id).unwrap();
        assert!(store.cache.contains_key(&id), "read should populate cache");
    }

    #[test]
    fn second_read_is_cache_hit() {
        let (_f, mut store) = open_store();
        let id = store.allocate().unwrap();
        store.write(id, leaf()).unwrap();
        store.cache.clear();

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
        store.cache.clear();

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
}
