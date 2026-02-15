use std::{
    borrow::Borrow,
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::prelude::MetadataExt,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

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

#[derive(Serialize, Deserialize)]
pub struct ZeroPage {
    // Contains metadata usefull to the pager

    // TODO: make this the head of a linked list to ensure it is a fixed size when encoding ZeroPage
    free_page_list: Vec<u32>,

    /// Root page number of the db_schema catalog table.
    /// This is the only root page tracked directly by the pager.
    /// All other table root pages are stored as rows in db_schema.
    schema_root_page: Option<u32>,
}

impl Default for ZeroPage {
    fn default() -> Self {
        Self {
            free_page_list: Default::default(),
            schema_root_page: None,
        }
    }
}

pub struct Pager {
    path: String,
    file: RefCell<File>,
}

impl std::fmt::Debug for Pager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pager")
            .field("path", &self.path)
            .field("file", &"<File>")
            .finish()
    }
}

const PAGE_SIZE: u64 = 2 << 11;

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

    fn get_zero_page(&self) -> Option<ZeroPage> {
        if self.get_file_size_pages() < 1 {
            None
        } else {
            Some(self.get_and_decode(0))
        }
    }

    fn set_zero_page(&mut self, zero: ZeroPage) {
        self.encode_and_set(0, zero).unwrap();
    }

    pub fn get<PageNo: Borrow<u32>>(&self, idx: PageNo) -> Page {
        // println!("Reading page {}", idx.borrow());
        let mut p = Page::default();
        let content = p.content.as_mut_slice();

        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE * (*idx.borrow() as u64);
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.read_exact(content).unwrap();

        p
    }

    pub fn get_and_decode<P: Borrow<P> + DeserializeOwned, PageNo: Borrow<u32>>(
        &self,
        idx: PageNo,
    ) -> P {
        let p = self.get(idx);
        ciborium::de::from_reader(&p.content[..]).unwrap()
    }

    pub fn set<P: Borrow<Page>, PageNo: Borrow<u32>>(&mut self, idx: PageNo, page: P) {
        // println!("Writing page {}", idx.borrow());
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE * (*idx.borrow() as u64);
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all(&page.borrow().content).unwrap();
    }

    pub fn encode_and_set<P: Borrow<P> + Serialize, PageNo: Borrow<u32>>(
        &mut self,
        idx: PageNo,
        v: P,
    ) -> Result<(), EncodingError> {
        let mut page = Page::default();
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

        self.set(idx, page);

        Ok(())
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
            // New page is the first page
            1
        } else {
            // We need to find the page allocation table in the first page and get a page from its free list

            let mut zero = self.get_zero_page().unwrap();
            let page_no = zero.free_page_list.pop();

            self.set_zero_page(zero);

            if let Some(page_no) = page_no {
                page_no
            } else {
                // If there are no pages in the free list we need to expand the filesize
                // TODO: For performance reasons, maybe increment number of pages by more than one?
                self.set_file_size_pages(num_pages + 1);

                num_pages
            }
        }
    }

    #[allow(dead_code)]
    pub fn dealocate(&mut self, idx: u32) {
        if idx == 0 {
            panic!("Cant dealloc page zero");
        }

        let mut zero = self.get_zero_page().unwrap();

        if zero.free_page_list.contains(&idx) {
            panic!("Free list already contains this page!");
        }

        zero.free_page_list.push(idx);

        self.set_zero_page(zero);
    }

    pub fn get_schema_root_page(&self) -> Option<u32> {
        let zero = self.get_zero_page()?;
        zero.schema_root_page
    }

    pub fn set_schema_root_page(&mut self, page: u32) {
        let mut zero = self.get_zero_page().unwrap();
        zero.schema_root_page = Some(page);
        self.set_zero_page(zero);
    }

    #[allow(dead_code)]
    pub fn debug(&self, message: &str) {
        for i in 0..self.get_file_size_pages() {
            let page: serde_json::Value = self.get_and_decode(i);

            println!("{message}: Page {i} : {page}");
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use super::Pager;

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

        pager.set(page_one_idx, &page_one_content);
        pager.set(page_two_idx, &page_two_content);

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
            pager.set(page_idx, &page_content);
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
        pager.set(page_d, &page_content);

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
        pager.set(page_idx, &page_content);

        // Read it back and verify
        let read_back = pager.get(page_idx);
        for i in 0..4096 {
            assert_eq!((i % 256) as u8, read_back.content[i]);
        }
    }
}
