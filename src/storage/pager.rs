use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::prelude::MetadataExt,
    rc::Rc,
};

use probe::probe;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::node::NodePage;

pub struct Page {
    // TODO: maybe share an existing open page
    content: [u8; PAGE_SIZE as usize],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            content: [0; PAGE_SIZE as usize],
        }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Self {
            content: self.content,
        }
    }
}

/// Linked list page for tracking free pages
#[derive(Serialize, Deserialize)]
struct FreeListPage {
    /// Next page in the free list chain (None if this is the last page)
    next: Option<u32>,
    /// Page IDs available for allocation (up to ~1000 per page)
    page_ids: Vec<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ZeroPage {
    // Contains metadata usefull to the pager
    /// Magic number to identify database files: 0x53514C69 ("SQLi")
    magic: u32,

    /// Format version: 0 = JSON (deprecated), 1 = CBOR with schema_root_page
    /// (deprecated), 2 = CBOR without schema_root_page (catalog always at page 1)
    format_version: u16,

    /// First page of the free list linked list (None if no free pages)
    free_list_head: Option<u32>,
}

impl Default for ZeroPage {
    fn default() -> Self {
        Self {
            magic: 0x53514C69, // "SQLi"
            format_version: 2, // CBOR format, catalog root hardcoded at page 1
            free_list_head: None,
        }
    }
}

pub struct Pager {
    path: String,
    file: RefCell<File>,
    /// In-memory page cache: page_number → (page_content, is_dirty)
    cache: RefCell<HashMap<u32, (Page, bool)>>,
    /// Decoded NodePage cache: avoids repeated CBOR deserialization
    decoded: RefCell<HashMap<u32, Rc<NodePage>>>,
}

impl std::fmt::Debug for Pager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pager")
            .field("path", &self.path)
            .field("file", &"<File>")
            .finish()
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        // Flush the file buffer to ensure OS-level buffering is committed.
        // With write-through caching, there are no dirty cache pages,
        // but the file handle may have unflushed OS buffers.
        self.file.borrow_mut().flush().unwrap();
    }
}

pub(crate) const PAGE_SIZE: u64 = 2 << 11;

#[derive(Debug)]
pub enum EncodingError {
    NotEnoughSpaceInPage,
    SerializationError(String),
}

impl Pager {
    pub fn new(path: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        Pager {
            path: path.to_owned(),
            file: RefCell::new(file),
            cache: RefCell::new(HashMap::new()),
            decoded: RefCell::new(HashMap::new()),
        }
    }

    pub fn get_file_size_pages(&self) -> u32 {
        let file = self.file.borrow();
        let file_size_bytes = file.metadata().unwrap().size();
        let num_pages = file_size_bytes / PAGE_SIZE;

        num_pages as u32
    }

    pub fn set_file_size_pages(&self, num_pages: u32) {
        let file = self.file.borrow();
        file.set_len(PAGE_SIZE * num_pages as u64).unwrap();
    }

    pub fn get_zero_page(&self) -> Option<ZeroPage> {
        if self.get_file_size_pages() < 1 {
            None
        } else {
            let page = self.get_and_decode(0);
            probe!(database, page_read_zero, 0u32);
            Some(page)
        }
    }

    fn set_zero_page(&mut self, zero: ZeroPage) {
        self.encode_and_set(0, zero).unwrap();
        probe!(database, page_write_zero, 0u32);
    }

    pub fn get<PageNo: Borrow<u32>>(&self, idx: PageNo) -> Page {
        let page_no = *idx.borrow();

        // Cache hit: return a copy of the cached page
        if let Some((page, _dirty)) = self.cache.borrow().get(&page_no) {
            probe!(database, page_read_cache_hit, page_no);
            return page.clone();
        }

        // Cache miss: read from disk, insert into cache, return a copy
        let mut p = Page::default();
        let content = p.content.as_mut_slice();

        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE * (page_no as u64);
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.read_exact(content).unwrap();
        drop(file);

        probe!(database, page_read_cache_miss, page_no);
        let ret = p.clone();
        self.cache.borrow_mut().insert(page_no, (p, false));
        ret
    }

    fn get_and_decode<P: Borrow<P> + DeserializeOwned, PageNo: Borrow<u32>>(
        &self,
        idx: PageNo,
    ) -> P {
        let p = self.get(idx);
        probe!(database, cbor_page_decode);
        ciborium::de::from_reader(&p.content[..]).unwrap()
    }

    /// Decode a page as a `NodePage`, firing the appropriate typed USDT probe
    /// (`page_read_leaf`, `page_read_interior`, or `page_read_overflow`) in one
    /// place rather than at every call site. This keeps probe site counts low
    /// enough that bpftrace can attach without crashing. Results are cached in
    /// `self.decoded` to avoid repeated CBOR deserialization; the cache is
    /// invalidated automatically on any write through `set`.
    pub fn get_and_decode_node<PageNo: Borrow<u32>>(&self, idx: PageNo) -> Rc<NodePage> {
        let page_no = *idx.borrow();
        let node = if let Some(page) = self.decoded.borrow().get(&page_no) {
            Rc::clone(page)
        } else {
            let node = Rc::new(self.get_and_decode(page_no));
            self.decoded.borrow_mut().insert(page_no, Rc::clone(&node));
            node
        };

        match node.as_ref() {
            NodePage::Leaf(_) => probe!(database, page_read_leaf, page_no),
            NodePage::Interior(_) => probe!(database, page_read_interior, page_no),
            NodePage::OverflowPage(_) => probe!(database, page_read_overflow, page_no),
        }

        node
    }

    pub fn set(&mut self, idx: u32, page: Page) {
        probe!(database, page_write, idx);
        // Write through: write to disk first, then move into cache (no clone)
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE * (idx as u64);
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all(&page.content).unwrap();
        drop(file);
        self.cache.borrow_mut().insert(idx, (page, false));
    }

    pub fn encode_and_set<P: Borrow<P> + Serialize, PageNo: Borrow<u32>>(
        &mut self,
        idx: PageNo,
        v: P,
    ) -> Result<(), EncodingError> {
        let mut page = Page::default();
        probe!(database, cbor_page_encode);
        let result = ciborium::ser::into_writer(v.borrow(), &mut &mut page.content[..]);

        match result {
            Err(e) => {
                // CBOR serialization errors typically indicate buffer overflow or data issues
                let err_str = e.to_string();
                if err_str.contains("failed to write whole buffer")
                    || err_str.contains("write zero")
                {
                    return Err(EncodingError::NotEnoughSpaceInPage);
                }
                return Err(EncodingError::SerializationError(format!(
                    "CBOR encoding failed: {}",
                    e
                )));
            }
            _ => {}
        };

        self.set(*idx.borrow(), page);

        Ok(())
    }

    /// Encode a `NodePage` to disk and populate the decoded cache (write-through).
    /// Prefer this over `encode_and_set` for all NodePage writes so that a
    /// subsequent read of the same page is always a cache hit.
    pub fn write_node_page(&mut self, idx: u32, page: Rc<NodePage>) -> Result<(), EncodingError> {
        #[cfg(debug_assertions)]
        if let Some(existing) = self.decoded.borrow().get(&idx) {
            debug_assert!(
                Rc::strong_count(existing) == 1,
                "writing page {} while {} callers still hold a reference",
                idx,
                Rc::strong_count(existing) - 1
            );
        }
        let result = self.encode_and_set(idx, &*page);
        if result.is_ok() {
            self.decoded.borrow_mut().insert(idx, page);
        }
        result
    }

    /// Remove the decoded cache entry and return sole Rc ownership to the caller.
    /// The strong_count assertion ensures no other caller is still holding a reference.
    fn take_decoded_node(&self, page_no: u32) -> Option<Rc<NodePage>> {
        let rc = self.decoded.borrow_mut().remove(&page_no)?;
        #[cfg(debug_assertions)]
        debug_assert!(
            Rc::strong_count(&rc) == 1,
            "taking page {} for mutation while {} callers hold references",
            page_no,
            Rc::strong_count(&rc) - 1
        );
        Some(rc)
    }

    /// Fetch page `page_no`, apply `f` to a mutable reference, then write the result back.
    ///
    /// Hot path (page in decoded cache): `take_decoded_node` gives sole ownership →
    /// `Rc::try_unwrap` succeeds → zero-copy mutation.
    ///
    /// Cold path (page not in decoded cache): falls back to a decode + clone (rare).
    ///
    /// Returns `(closure_result, Ok(()))` on success.
    /// Returns `(closure_result, Err((error, page)))` on encoding failure — the owned
    /// `NodePage` is returned so the caller can split it without an extra clone.
    pub fn mutate_node<R>(
        &mut self,
        page_no: u32,
        f: impl FnOnce(&mut NodePage) -> R,
    ) -> (R, Result<(), (EncodingError, NodePage)>) {
        let rc = self
            .take_decoded_node(page_no)
            .unwrap_or_else(|| self.get_and_decode_node(page_no));
        // Hot path: strong_count == 1 (removed from cache) → unwrap succeeds, no clone.
        // Cold path: strong_count == 2 (still in cache from get_and_decode_node) → clone.
        let mut page = Rc::try_unwrap(rc).unwrap_or_else(|r| (*r).clone());
        let r = f(&mut page);
        let result = self.encode_and_set(page_no, &page);
        match result {
            Ok(()) => {
                self.decoded.borrow_mut().insert(page_no, Rc::new(page));
                (r, Ok(()))
            }
            Err(e) => (r, Err((e, page))),
        }
    }

    pub fn allocate(&mut self) -> u32 {
        let num_pages = self.get_file_size_pages();

        // we dont have any pages
        if num_pages == 0 {
            // Allocate two pages, one for the pager and one to return to the caller
            self.set_file_size_pages(2);

            // Write out new zero page
            let zero = ZeroPage::default();
            self.set_zero_page(zero);

            probe!(database, page_allocate, 1);

            // New page is the first page
            1
        } else {
            let mut zero = self.get_zero_page().unwrap();

            // Try to get a page from the free list
            if let Some(head_page_no) = zero.free_list_head {
                // Read the free list head page
                let mut free_list_page: FreeListPage = self.get_and_decode(head_page_no);
                probe!(database, page_read_freelist, head_page_no);

                // Pop a page ID from the list
                if let Some(page_id) = free_list_page.page_ids.pop() {
                    // Update the free list page
                    self.encode_and_set(head_page_no, &free_list_page).unwrap();

                    return page_id;
                } else {
                    // This free list page is empty, reclaim it or move to next
                    zero.free_list_head = free_list_page.next;
                    self.set_zero_page(zero);

                    // Return the page that was being used as FreeListPage container
                    return head_page_no;
                }
            }

            // No free pages available, expand the file
            self.set_file_size_pages(num_pages + 1);

            probe!(database, page_allocate, num_pages);

            num_pages
        }
    }

    #[allow(dead_code)]
    pub fn dealocate(&mut self, idx: u32) {
        // probe!(database, page_deallocate, idx);  -- removed: dead code causes invalid ELF note addresses
        if idx == 0 {
            panic!("Cant dealloc page zero");
        }
        self.decoded.borrow_mut().remove(&idx);

        let mut zero = self.get_zero_page().unwrap();

        // If there's a free list head, try to add to it
        if let Some(head_page_no) = zero.free_list_head {
            let mut free_list_page: FreeListPage = self.get_and_decode(head_page_no);
            // probe!(database, page_read_freelist, head_page_no);  -- removed: same reason

            // Check if this page can fit more entries (~1000 is a safe limit)
            if free_list_page.page_ids.len() < 1000 {
                free_list_page.page_ids.push(idx);
                self.encode_and_set(head_page_no, &free_list_page).unwrap();
                return;
            }
        }

        // Need to create a new free list page
        // Use the page being freed as the new FreeListPage container
        let new_free_list_page = FreeListPage {
            next: zero.free_list_head,
            page_ids: vec![], // Empty - this page becomes the container
        };

        self.encode_and_set(idx, &new_free_list_page).unwrap();

        // Update zero page to point to new head
        zero.free_list_head = Some(idx);
        self.set_zero_page(zero);
    }

    /// Validate the database format version. Panics if unsupported.
    pub fn validate_format_version(&self) {
        if let Some(zero) = self.get_zero_page() {
            match zero.format_version {
                0 => panic!(
                    "Database format version 0 (JSON) is no longer supported. \
                     Please recreate your database. Pre-1.0 databases are not \
                     backwards compatible."
                ),
                1 => panic!(
                    "Database format version 1 is no longer supported. \
                     Please recreate your database. The catalog root page is now \
                     hardcoded at page 1 rather than stored in the file header."
                ),
                2 => { /* current CBOR format - continue normally */ }
                v => panic!(
                    "Unknown database format version {}. \
                     This database may have been created by a newer version.",
                    v
                ),
            }
        }
    }

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        // probes removed from this dead code function to avoid invalid ELF note addresses:
        //   probe!(database, page_read_zero, 0u32);
        //   probe!(database, page_read_leaf, i);
        //   probe!(database, page_read_interior, i);
        //   probe!(database, page_read_overflow, i);
        for i in 0..self.get_file_size_pages() {
            if i == 0 {
                let zero_page: ZeroPage = self.get_and_decode(0);
                println!("{message}: Page {i} (ZeroPage): {zero_page:?}");
            } else {
                let node_page = self.get_and_decode_node(i);
                println!("{message}: Page {i}: {node_page:?}");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use super::Pager;
    use crate::storage::node::NodePage;

    #[test]
    fn simple() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        assert_eq!(0, pager.get_file_size_pages());

        let page_one_idx = pager.allocate();

        let page_two_idx = pager.allocate();

        assert_eq!(3, pager.get_file_size_pages());

        let mut page_one_content = pager.get(page_one_idx);
        let mut page_two_content = pager.get(page_two_idx);

        page_one_content.content[0] = 10;
        page_one_content.content[10] = 10;

        page_two_content.content[0] = 20;
        page_two_content.content[20] = 20;

        pager.set(page_one_idx, page_one_content);
        pager.set(page_two_idx, page_two_content);

        // Re open file from disk
        let pager = Pager::new(path);

        assert_eq!(3, pager.get_file_size_pages());

        let page_one_content = pager.get(page_one_idx);
        let page_two_content = pager.get(page_two_idx);

        assert_eq!(10, page_one_content.content[0]);
        assert_eq!(10, page_one_content.content[10]);

        assert_eq!(20, page_two_content.content[0]);
        assert_eq!(20, page_two_content.content[20]);
    }

    #[test]
    fn free_list() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        let a = pager.allocate();
        let _b = pager.allocate();
        let c = pager.allocate();
        let _d = pager.allocate();
        let e = pager.allocate();
        let f = pager.allocate();

        let max_size = pager.get_file_size_pages();

        pager.dealocate(a);
        pager.dealocate(c);
        pager.dealocate(e);
        pager.dealocate(f);

        // no shrinking of underlying file
        assert_eq!(max_size, pager.get_file_size_pages());

        let _a2 = pager.allocate();
        let _c2 = pager.allocate();
        let _e2 = pager.allocate();
        let _f2 = pager.allocate();

        // no further allocation needed, dealocated pages reused
        assert_eq!(max_size, pager.get_file_size_pages());

        // allocate one more page
        let _g = pager.allocate();

        // more pages allocated
        assert_eq!(max_size + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_pager_persistence() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        // Write pages to a file
        {
            let mut pager = Pager::new(path);
            let page_idx = pager.allocate();
            let mut page_content = pager.get(page_idx);
            page_content.content[0] = 42;
            page_content.content[100] = 99;
            pager.set(page_idx, page_content);
        }

        // Drop the pager, create a new one on the same file
        {
            let pager = Pager::new(path);
            let page_content = pager.get(1); // First allocated page is always 1
            assert_eq!(42, page_content.content[0]);
            assert_eq!(99, page_content.content[100]);
        }
    }

    #[test]
    fn test_pager_free_list_roundtrip() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        // Allocate 3 pages
        let page_a = pager.allocate();
        let page_b = pager.allocate();
        let page_c = pager.allocate();

        assert_eq!(page_a, 1);
        assert_eq!(page_b, 2);
        assert_eq!(page_c, 3);

        // Deallocate middle page
        pager.dealocate(page_b);

        // Allocate again - should reuse the freed page
        let page_d = pager.allocate();
        assert_eq!(page_b, page_d, "Should reuse the deallocated page");

        // Verify we can write to and read from the reused page
        let mut page_content = pager.get(page_d);
        page_content.content[0] = 123;
        pager.set(page_d, page_content);

        let read_back = pager.get(page_d);
        assert_eq!(123, read_back.content[0]);
    }

    #[test]
    fn test_pager_page_boundary() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);
        let page_idx = pager.allocate();

        // Write exactly 4096 bytes to a page
        let mut page_content = pager.get(page_idx);
        for i in 0..4096 {
            page_content.content[i] = (i % 256) as u8;
        }
        pager.set(page_idx, page_content);

        // Read it back and verify
        let read_back = pager.get(page_idx);
        for i in 0..4096 {
            assert_eq!((i % 256) as u8, read_back.content[i]);
        }
    }

    #[test]
    fn test_multi_page_free_list() {
        // Test that free list can span multiple FreeListPages (>1000 entries)
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        // Allocate 1100 pages (capacity is 1000; 1100 spans exactly 2 FreeListPages)
        let mut allocated = Vec::new();
        for _ in 0..1100 {
            allocated.push(pager.allocate());
        }

        let size_after_alloc = pager.get_file_size_pages();

        // Deallocate all 1100 pages (should create multiple FreeListPages)
        for page in allocated.clone() {
            pager.dealocate(page);
        }

        // File size should not have grown (freed pages used as FreeListPage containers)
        assert_eq!(size_after_alloc, pager.get_file_size_pages());

        // Re-allocate all 1100 pages - should reuse freed pages
        let mut reallocated = Vec::new();
        for _ in 0..1100 {
            reallocated.push(pager.allocate());
        }

        // File size should still be the same (no new pages needed)
        assert_eq!(size_after_alloc, pager.get_file_size_pages());

        // All pages should be reused (though possibly in different order)
        let mut allocated_sorted = allocated.clone();
        allocated_sorted.sort();
        let mut reallocated_sorted = reallocated.clone();
        reallocated_sorted.sort();
        assert_eq!(allocated_sorted, reallocated_sorted);
    }

    #[test]
    fn test_large_scale_alloc_dealloc() {
        // Test allocating and deallocating 2000+ pages
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        // Allocate 500 pages
        let mut pages = Vec::new();
        for _ in 0..500 {
            pages.push(pager.allocate());
        }

        let max_size = pager.get_file_size_pages();
        assert!(max_size >= 501); // At least 500 data pages + zero page

        // Deallocate half of them
        for i in (0..500).step_by(2) {
            pager.dealocate(pages[i]);
        }

        // File size should not change
        assert_eq!(max_size, pager.get_file_size_pages());

        // Allocate 250 new pages - should reuse the freed ones
        for _ in 0..250 {
            pager.allocate();
        }

        // File size should still be the same
        assert_eq!(max_size, pager.get_file_size_pages());

        // Allocate one more - should expand the file
        pager.allocate();
        assert_eq!(max_size + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_empty_free_list() {
        // Test behavior when free list is empty
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        // Allocate some pages
        let a = pager.allocate();
        let b = pager.allocate();
        let c = pager.allocate();

        let size1 = pager.get_file_size_pages();

        // Deallocate them
        pager.dealocate(a);
        pager.dealocate(b);
        pager.dealocate(c);

        // Allocate them back (empties the free list)
        pager.allocate();
        pager.allocate();
        pager.allocate();

        // Free list should now be empty
        // Allocating another page should expand the file
        pager.allocate();
        assert_eq!(size1 + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_free_list_persistence_large() {
        // Test that large free lists persist across database close/reopen
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let max_size;
        {
            let mut pager = Pager::new(path);

            // Allocate 500 pages
            let mut pages = Vec::new();
            for _ in 0..500 {
                pages.push(pager.allocate());
            }

            max_size = pager.get_file_size_pages();

            // Deallocate all of them
            for page in pages {
                pager.dealocate(page);
            }
        }

        // Reopen and verify free list is intact
        {
            let mut pager = Pager::new(path);

            // File size should be unchanged
            assert_eq!(max_size, pager.get_file_size_pages());

            // Should be able to allocate 500 pages without expanding file
            for _ in 0..500 {
                pager.allocate();
            }

            assert_eq!(max_size, pager.get_file_size_pages());

            // Next allocation should expand
            pager.allocate();
            assert_eq!(max_size + 1, pager.get_file_size_pages());
        }
    }

    #[test]
    fn test_free_last_allocated_page() {
        // Edge case: free the most recently allocated page
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        let a = pager.allocate();
        let b = pager.allocate();
        let c = pager.allocate();

        let size = pager.get_file_size_pages();

        // Free the last allocated page
        pager.dealocate(c);

        // File should not shrink
        assert_eq!(size, pager.get_file_size_pages());

        // Should be able to reuse it
        let c2 = pager.allocate();
        assert_eq!(c, c2);

        // File should not have expanded
        assert_eq!(size, pager.get_file_size_pages());

        // Free first allocated page
        pager.dealocate(a);

        // Allocate should reuse it
        let a2 = pager.allocate();
        assert_eq!(a, a2);

        // Verify b is still valid
        let page_b = pager.get(b);
        assert_eq!(page_b.content[0], 0); // Should be zeros (never written to)
    }

    #[test]
    fn test_page_cache_read_hit() {
        // After a write, a subsequent read should return the cached value
        // (not go to disk) and return the correct data.
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);
        let page_idx = pager.allocate();

        // Write a page
        let mut page = pager.get(page_idx);
        page.content[0] = 77;
        page.content[255] = 88;
        pager.set(page_idx, page);

        // Read it back — should be served from cache with the written values
        let read_back = pager.get(page_idx);
        assert_eq!(77, read_back.content[0]);
        assert_eq!(88, read_back.content[255]);
        assert_eq!(0, read_back.content[1]); // untouched bytes are zero
    }

    #[test]
    fn test_page_cache_repeated_reads() {
        // Multiple reads of the same page should return consistent data.
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);
        let page_idx = pager.allocate();

        let mut page = pager.get(page_idx);
        page.content[42] = 123;
        pager.set(page_idx, page);

        // Read the same page multiple times
        for _ in 0..10 {
            let p = pager.get(page_idx);
            assert_eq!(123, p.content[42]);
        }
    }

    #[test]
    fn test_decoded_cache_hit() {
        // get_and_decode_node called twice on the same page should populate the decoded cache
        // so the second call returns the cached struct without re-decoding.
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        use crate::storage::node::LeafNodePage;

        let mut pager = Pager::new(path);
        let idx = pager.allocate();

        // Write a NodePage
        let node = NodePage::Leaf(LeafNodePage::default());
        pager.encode_and_set(idx, &node).unwrap();

        // Clear the decoded cache to simulate a fresh read
        pager.decoded.borrow_mut().clear();
        assert_eq!(0, pager.decoded.borrow().len());

        // First read — should miss the cache and populate it
        let _p1 = pager.get_and_decode_node(idx);
        assert_eq!(
            1,
            pager.decoded.borrow().len(),
            "cache should have one entry after first read"
        );

        // Second read — should hit the cache (length stays 1)
        let _p2 = pager.get_and_decode_node(idx);
        assert_eq!(
            1,
            pager.decoded.borrow().len(),
            "cache length unchanged on second read"
        );
    }

    #[test]
    fn test_decoded_cache_evicted_on_deallocate() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        use crate::storage::node::LeafNodePage;

        let mut pager = Pager::new(path);
        let idx = pager.allocate();

        let node = NodePage::Leaf(LeafNodePage::default());
        pager.encode_and_set(idx, &node).unwrap();
        let _ = pager.get_and_decode_node(idx); // populate cache

        assert!(pager.decoded.borrow().contains_key(&idx));

        pager.dealocate(idx);

        assert!(
            !pager.decoded.borrow().contains_key(&idx),
            "decoded cache must evict on deallocate"
        );
    }

    #[test]
    fn test_page_cache_correct_after_mutations() {
        // After writing multiple pages, all should be readable with correct data.
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();

        let mut pager = Pager::new(path);

        let p1 = pager.allocate();
        let p2 = pager.allocate();
        let p3 = pager.allocate();

        for (idx, page_no) in [p1, p2, p3].iter().enumerate() {
            let mut page = pager.get(*page_no);
            page.content[0] = (idx + 1) as u8;
            pager.set(*page_no, page);
        }

        assert_eq!(1, pager.get(p1).content[0]);
        assert_eq!(2, pager.get(p2).content[0]);
        assert_eq!(3, pager.get(p3).content[0]);
    }
}
