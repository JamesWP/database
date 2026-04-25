use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;

#[cfg(not(target_arch = "wasm32"))]
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek},
    os::unix::prelude::MetadataExt,
};

use probe::probe;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::page_id::PageId;

pub(super) const PAGE_SIZE: u64 = 2 << 11; // 4096 bytes
const PAGE_SIZE_USIZE: usize = PAGE_SIZE as usize;

/// Linked list page for tracking free pages
#[derive(Serialize, Deserialize)]
struct FreeListPage {
    /// Next page in the free list chain (None if this is the last page)
    next: Option<u32>,
    /// Page IDs available for allocation (up to ~1000 per page)
    page_ids: Vec<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct ZeroPage {
    /// Magic number to identify database files: 0x53514C69 ("SQLi")
    magic: u32,

    /// Format version: 0 = JSON (deprecated), 1 = CBOR with schema_root_page
    /// (deprecated), 2 = CBOR without schema_root_page (catalog always at page 1)
    pub(super) format_version: u16,

    /// First page of the free list linked list (None if no free pages)
    free_list_head: Option<u32>,
}

impl Default for ZeroPage {
    fn default() -> Self {
        Self {
            magic: 0x53514C69, // "SQLi"
            format_version: 4, // CBOR format v4: Blob values serialised as CBOR byte strings
            free_list_head: None,
        }
    }
}

enum PagerStorage {
    #[cfg(not(target_arch = "wasm32"))]
    File {
        path: String,
        file: RefCell<File>,
    },
    Memory(RefCell<Vec<[u8; PAGE_SIZE_USIZE]>>),
}

pub(super) struct Pager {
    storage: PagerStorage,
}

impl std::fmt::Debug for Pager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { path, .. } => f.debug_struct("Pager").field("path", path).finish(),
            PagerStorage::Memory(pages) => f
                .debug_struct("Pager")
                .field("pages", &pages.borrow().len())
                .finish(),
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

impl Pager {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn new(path: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        Pager {
            storage: PagerStorage::File {
                path: path.to_owned(),
                file: RefCell::new(file),
            },
        }
    }

    pub(super) fn new_in_memory() -> Pager {
        Pager {
            storage: PagerStorage::Memory(RefCell::new(Vec::new())),
        }
    }

    pub(super) fn get_file_size_pages(&self) -> u32 {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { file, .. } => {
                let file = file.borrow();
                let file_size_bytes = file.metadata().unwrap().size();
                (file_size_bytes / PAGE_SIZE) as u32
            }
            PagerStorage::Memory(pages) => pages.borrow().len() as u32,
        }
    }

    fn set_file_size_pages(&self, num_pages: u32) {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { file, .. } => {
                let file = file.borrow();
                file.set_len(PAGE_SIZE * num_pages as u64).unwrap();
            }
            PagerStorage::Memory(pages) => {
                pages
                    .borrow_mut()
                    .resize(num_pages as usize, [0u8; PAGE_SIZE_USIZE]);
            }
        }
    }

    #[inline(never)]
    pub(super) fn get_zero_page(&self) -> Option<ZeroPage> {
        if self.get_file_size_pages() < 1 {
            return None;
        }
        let bytes = self.read_bytes(0);
        probe!(database, page_read_zero, 0u32);
        probe!(database, page_read, 0u32);
        Some(cbor_decode(&bytes))
    }

    #[inline(never)]
    fn set_zero_page(&mut self, zero: ZeroPage) {
        let bytes = cbor_encode(&zero).expect("ZeroPage serialization failed");
        self.write_bytes(0, &bytes);
        probe!(database, page_write_zero, 0u32);
    }

    /// Read a raw page from disk (no cache).
    #[inline(never)]
    pub(super) fn read_raw(&self, id: PageId) -> [u8; PAGE_SIZE_USIZE] {
        let page_no = id.as_u32();
        probe!(database, page_read_cache_miss, page_no);
        probe!(database, page_read, page_no);
        self.read_bytes(page_no)
    }

    fn read_bytes(&self, page_no: u32) -> [u8; PAGE_SIZE_USIZE] {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { file, .. } => {
                let mut bytes = [0u8; PAGE_SIZE_USIZE];
                let mut file = file.borrow_mut();
                let offset = PAGE_SIZE * (page_no as u64);
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                bytes
            }
            PagerStorage::Memory(pages) => pages.borrow()[page_no as usize],
        }
    }

    /// Write raw page bytes to disk.
    #[inline(never)]
    pub(super) fn write_raw(&mut self, id: PageId, bytes: &[u8; PAGE_SIZE_USIZE]) {
        probe!(database, page_write, id.as_u32());
        self.write_bytes(id.as_u32(), bytes);
    }

    fn write_bytes(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE_USIZE]) {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { file, .. } => {
                let mut file = file.borrow_mut();
                let offset = PAGE_SIZE * (page_no as u64);
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                file.write_all(bytes).unwrap();
            }
            PagerStorage::Memory(pages) => {
                pages.borrow_mut()[page_no as usize] = *bytes;
            }
        }
    }

    // ── page lifecycle ────────────────────────────────────────────────────────

    #[inline(never)]
    pub(super) fn allocate(&mut self) -> PageId {
        let num_pages = self.get_file_size_pages();

        if num_pages == 0 {
            // Empty file: allocate page 0 (ZeroPage) and page 1 (first data page).
            self.set_file_size_pages(2);
            self.set_zero_page(ZeroPage::default());
            probe!(database, page_allocate, 1u32);
            PageId(1)
        } else {
            let mut zero = self.get_zero_page().unwrap();

            // Try to reclaim a page from the free list.
            if let Some(head_page_no) = zero.free_list_head {
                let bytes = self.read_bytes(head_page_no);
                let mut free_list_page: FreeListPage = cbor_decode(&bytes);
                probe!(database, page_read_freelist, head_page_no);
                probe!(database, page_read, head_page_no);

                if let Some(page_id) = free_list_page.page_ids.pop() {
                    // Update the partially-drained free list page.
                    let updated = cbor_encode(&free_list_page).expect("FreeListPage encode failed");
                    self.write_bytes(head_page_no, &updated);
                    probe!(database, page_write_freelist, head_page_no);
                    return PageId(page_id);
                } else {
                    // Free list page is now empty; reclaim it as the returned page.
                    zero.free_list_head = free_list_page.next;
                    self.set_zero_page(zero);
                    return PageId(head_page_no);
                }
            }

            // No free pages: expand the file.
            self.set_file_size_pages(num_pages + 1);
            probe!(database, page_allocate, num_pages);
            PageId(num_pages)
        }
    }

    #[inline(never)]
    pub(super) fn free(&mut self, id: PageId) {
        let idx = id.as_u32();
        if idx == 0 {
            panic!("Cannot free page zero");
        }

        // Note: probes omitted here — free() is dead code in insert-only workloads
        // and LTO eliminates it, causing invalid ELF USDT note addresses.

        let mut zero = self.get_zero_page().unwrap();

        if let Some(head_page_no) = zero.free_list_head {
            let bytes = self.read_bytes(head_page_no);
            let mut free_list_page: FreeListPage = cbor_decode(&bytes);

            if free_list_page.page_ids.len() < 1000 {
                free_list_page.page_ids.push(idx);
                let updated = cbor_encode(&free_list_page).expect("FreeListPage encode failed");
                self.write_bytes(head_page_no, &updated);
                return;
            }
        }

        // Create a new free list page using the freed page as its container.
        let new_free_list_page = FreeListPage {
            next: zero.free_list_head,
            page_ids: vec![],
        };
        let bytes = cbor_encode(&new_free_list_page).expect("FreeListPage encode failed");
        self.write_bytes(idx, &bytes);

        zero.free_list_head = Some(idx);
        self.set_zero_page(zero);
    }

    pub(super) fn flush(&mut self) -> std::io::Result<()> {
        match &self.storage {
            #[cfg(not(target_arch = "wasm32"))]
            PagerStorage::File { file, .. } => file.borrow_mut().flush(),
            PagerStorage::Memory(_) => Ok(()),
        }
    }
}

// ── internal CBOR helpers (ZeroPage and FreeListPage only) ───────────────────

fn cbor_encode<T: Serialize>(v: &T) -> Option<[u8; PAGE_SIZE_USIZE]> {
    let mut bytes = [0u8; PAGE_SIZE_USIZE];
    ciborium::ser::into_writer(v, &mut &mut bytes[..]).ok()?;
    Some(bytes)
}

fn cbor_decode<T: DeserializeOwned>(bytes: &[u8; PAGE_SIZE_USIZE]) -> T {
    ciborium::de::from_reader(&bytes[..]).unwrap()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use super::{PageId, Pager, PAGE_SIZE};

    fn open_pager() -> (NamedTempFile, Pager) {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let pager = Pager::new(path);
        (file, pager)
    }

    #[test]
    fn simple() {
        let (f, mut pager) = open_pager();

        assert_eq!(0, pager.get_file_size_pages());

        let page_one_idx = pager.allocate();
        let page_two_idx = pager.allocate();

        assert_eq!(3, pager.get_file_size_pages());

        let mut p1 = [0u8; PAGE_SIZE as usize];
        p1[0] = 10;
        p1[10] = 10;
        pager.write_raw(page_one_idx, &p1);

        let mut p2 = [0u8; PAGE_SIZE as usize];
        p2[0] = 20;
        p2[20] = 20;
        pager.write_raw(page_two_idx, &p2);

        // Re-open file from disk.
        let path = f.path().to_str().unwrap().to_string();
        drop(pager);
        let pager = Pager::new(&path);

        assert_eq!(3, pager.get_file_size_pages());

        let r1 = pager.read_raw(page_one_idx);
        let r2 = pager.read_raw(page_two_idx);

        assert_eq!(10, r1[0]);
        assert_eq!(10, r1[10]);

        assert_eq!(20, r2[0]);
        assert_eq!(20, r2[20]);
    }

    #[test]
    fn free_list() {
        let (_f, mut pager) = open_pager();

        let a = pager.allocate();
        let _b = pager.allocate();
        let c = pager.allocate();
        let _d = pager.allocate();
        let e = pager.allocate();
        let f = pager.allocate();

        let max_size = pager.get_file_size_pages();

        pager.free(a);
        pager.free(c);
        pager.free(e);
        pager.free(f);

        // No shrinking of underlying file.
        assert_eq!(max_size, pager.get_file_size_pages());

        let _a2 = pager.allocate();
        let _c2 = pager.allocate();
        let _e2 = pager.allocate();
        let _f2 = pager.allocate();

        // No further allocation needed; freed pages are reused.
        assert_eq!(max_size, pager.get_file_size_pages());

        // Allocate one more page.
        let _g = pager.allocate();
        assert_eq!(max_size + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_pager_persistence() {
        let (f, mut pager) = open_pager();
        let path = f.path().to_str().unwrap().to_string();

        let page_idx = pager.allocate();
        let mut bytes = [0u8; PAGE_SIZE as usize];
        bytes[0] = 42;
        bytes[100] = 99;
        pager.write_raw(page_idx, &bytes);
        drop(pager);

        let pager = Pager::new(&path);
        let r = pager.read_raw(PageId(1));
        assert_eq!(42, r[0]);
        assert_eq!(99, r[100]);
    }

    #[test]
    fn test_pager_free_list_roundtrip() {
        let (_f, mut pager) = open_pager();

        let page_a = pager.allocate();
        let page_b = pager.allocate();
        let page_c = pager.allocate();

        assert_eq!(PageId(1), page_a);
        assert_eq!(PageId(2), page_b);
        assert_eq!(PageId(3), page_c);

        pager.free(page_b);

        let page_d = pager.allocate();
        assert_eq!(page_b, page_d, "Should reuse the freed page");

        let mut bytes = [0u8; PAGE_SIZE as usize];
        bytes[0] = 123;
        pager.write_raw(page_d, &bytes);
        let r = pager.read_raw(page_d);
        assert_eq!(123, r[0]);
    }

    #[test]
    fn test_pager_page_boundary() {
        let (_f, mut pager) = open_pager();
        let page_idx = pager.allocate();

        let mut bytes = [0u8; PAGE_SIZE as usize];
        for i in 0..PAGE_SIZE as usize {
            bytes[i] = (i % 256) as u8;
        }
        pager.write_raw(page_idx, &bytes);

        let r = pager.read_raw(page_idx);
        for i in 0..PAGE_SIZE as usize {
            assert_eq!((i % 256) as u8, r[i]);
        }
    }

    #[test]
    fn test_multi_page_free_list() {
        let (_f, mut pager) = open_pager();

        let mut allocated = Vec::new();
        for _ in 0..1100 {
            allocated.push(pager.allocate());
        }
        let size_after_alloc = pager.get_file_size_pages();

        for page in allocated.clone() {
            pager.free(page);
        }
        assert_eq!(size_after_alloc, pager.get_file_size_pages());

        let mut reallocated = Vec::new();
        for _ in 0..1100 {
            reallocated.push(pager.allocate());
        }
        assert_eq!(size_after_alloc, pager.get_file_size_pages());

        let mut a = allocated.clone();
        a.sort_by_key(|p| p.as_u32());
        let mut r = reallocated.clone();
        r.sort_by_key(|p| p.as_u32());
        assert_eq!(a, r);
    }

    #[test]
    fn test_large_scale_alloc_dealloc() {
        let (_f, mut pager) = open_pager();

        let mut pages = Vec::new();
        for _ in 0..500 {
            pages.push(pager.allocate());
        }

        let max_size = pager.get_file_size_pages();
        assert!(max_size >= 501);

        for i in (0..500).step_by(2) {
            pager.free(pages[i]);
        }
        assert_eq!(max_size, pager.get_file_size_pages());

        for _ in 0..250 {
            pager.allocate();
        }
        assert_eq!(max_size, pager.get_file_size_pages());

        pager.allocate();
        assert_eq!(max_size + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_empty_free_list() {
        let (_f, mut pager) = open_pager();

        let a = pager.allocate();
        let b = pager.allocate();
        let c = pager.allocate();
        let size1 = pager.get_file_size_pages();

        pager.free(a);
        pager.free(b);
        pager.free(c);

        pager.allocate();
        pager.allocate();
        pager.allocate();

        pager.allocate();
        assert_eq!(size1 + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_free_list_persistence_large() {
        let (f, mut pager) = open_pager();
        let path = f.path().to_str().unwrap().to_string();

        let mut pages = Vec::new();
        for _ in 0..500 {
            pages.push(pager.allocate());
        }
        let max_size = pager.get_file_size_pages();

        for page in pages {
            pager.free(page);
        }
        drop(pager);

        let mut pager = Pager::new(&path);
        assert_eq!(max_size, pager.get_file_size_pages());

        for _ in 0..500 {
            pager.allocate();
        }
        assert_eq!(max_size, pager.get_file_size_pages());

        pager.allocate();
        assert_eq!(max_size + 1, pager.get_file_size_pages());
    }

    #[test]
    fn test_free_last_allocated_page() {
        let (_f, mut pager) = open_pager();

        let a = pager.allocate();
        let b = pager.allocate();
        let c = pager.allocate();
        let size = pager.get_file_size_pages();

        pager.free(c);
        assert_eq!(size, pager.get_file_size_pages());

        let c2 = pager.allocate();
        assert_eq!(c, c2);
        assert_eq!(size, pager.get_file_size_pages());

        pager.free(a);
        let a2 = pager.allocate();
        assert_eq!(a, a2);

        let rb = pager.read_raw(b);
        assert_eq!(rb[0], 0);
    }

    #[test]
    fn test_read_write_roundtrip() {
        let (_f, mut pager) = open_pager();
        let p1 = pager.allocate();
        let p2 = pager.allocate();
        let p3 = pager.allocate();

        for (idx, page_id) in [p1, p2, p3].iter().enumerate() {
            let mut bytes = [0u8; PAGE_SIZE as usize];
            bytes[0] = (idx + 1) as u8;
            pager.write_raw(*page_id, &bytes);
        }

        assert_eq!(1, pager.read_raw(p1)[0]);
        assert_eq!(2, pager.read_raw(p2)[0]);
        assert_eq!(3, pager.read_raw(p3)[0]);
    }
}
