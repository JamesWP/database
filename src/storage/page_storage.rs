pub const PAGE_SIZE: usize = 4096;

/// Synchronous page-I/O interface.
///
/// Implementors store and retrieve fixed-size (4096-byte) pages identified
/// by a 0-based u32 index. The interface is intentionally synchronous; async
/// backends (S3, IndexedDB) must implement blocking via Atomics.wait() in a
/// SharedArrayBuffer-based Worker.
pub trait PageStorage {
    fn page_count(&self) -> u32;
    fn set_page_count(&mut self, count: u32);
    /// Read page `page_no`. Panics if `page_no >= page_count()`.
    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE];
    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]);
    fn flush(&mut self) -> std::io::Result<()>;
}

#[cfg(not(target_arch = "wasm32"))]
pub struct FilePageStorage {
    file: std::cell::RefCell<std::fs::File>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FilePageStorage {
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            file: std::cell::RefCell::new(file),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PageStorage for FilePageStorage {
    fn page_count(&self) -> u32 {
        use std::os::unix::prelude::MetadataExt;
        let file = self.file.borrow();
        let size = file.metadata().unwrap().size();
        (size / PAGE_SIZE as u64) as u32
    }

    fn set_page_count(&mut self, count: u32) {
        self.file
            .borrow()
            .set_len(PAGE_SIZE as u64 * count as u64)
            .unwrap();
    }

    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE] {
        use std::io::{Read, Seek};
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE as u64 * page_no as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        let mut bytes = [0u8; PAGE_SIZE];
        file.read_exact(&mut bytes).unwrap();
        bytes
    }

    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        use std::io::{Seek, Write};
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE as u64 * page_no as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all(bytes).unwrap();
    }

    fn flush(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        self.file.borrow_mut().flush()
    }
}

pub struct MemoryPageStorage {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl MemoryPageStorage {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }
}

impl Default for MemoryPageStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl PageStorage for MemoryPageStorage {
    fn page_count(&self) -> u32 {
        self.pages.len() as u32
    }

    fn set_page_count(&mut self, n: u32) {
        self.pages.resize(n as usize, [0u8; PAGE_SIZE]);
    }

    fn read_page(&self, n: u32) -> [u8; PAGE_SIZE] {
        self.pages[n as usize]
    }

    fn write_page(&mut self, n: u32, bytes: &[u8; PAGE_SIZE]) {
        self.pages[n as usize] = *bytes;
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_page_storage_roundtrip() {
        let mut s = MemoryPageStorage::new();
        assert_eq!(s.page_count(), 0);
        s.set_page_count(2);
        assert_eq!(s.page_count(), 2);
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 42;
        s.write_page(1, &page);
        let r = s.read_page(1);
        assert_eq!(r[0], 42);
    }
}
