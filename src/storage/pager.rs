use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    os::unix::prelude::MetadataExt,
    path::Path,
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

    // contains the root pages for the given entities
    root_pages: HashMap<String, u32>,
}

impl Default for ZeroPage {
    fn default() -> Self {
        Self {
            free_page_list: Default::default(),
            root_pages: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Pager {
    path: String,
}

const PAGE_SIZE: u64 = 2 << 11;

#[derive(Debug)]
pub enum EncodingError {
    NotEnoughSpaceInPage,
}

impl Pager {
    pub fn new(path: &str) -> Pager {
        Pager {
            path: path.to_owned(),
        }
    }

    pub fn get_file_size_pages(&self) -> u32 {
        let path = Path::new(&self.path);
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(path)
            .unwrap();
        let file_size_bytes = file.metadata().unwrap().size();
        let num_pages = file_size_bytes / PAGE_SIZE;

        num_pages as u32
    }

    pub fn set_file_size_pages(&self, num_pages: u32) {
        let path = Path::new(&self.path);
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .open(path)
            .unwrap();

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

    fn file_at_page_readonly(&self, idx: u32) -> File {
        let path = Path::new(&self.path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(path)
            .unwrap();
        let seek = PAGE_SIZE * idx as u64;
        file.seek(std::io::SeekFrom::Start(seek)).unwrap();

        file
    }

    fn file_at_page_write(&mut self, idx: u32) -> File {
        let path = Path::new(&self.path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let seek = PAGE_SIZE * idx as u64;
        file.seek(std::io::SeekFrom::Start(seek)).unwrap();

        file
    }

    pub fn get<PageNo: Borrow<u32>>(&self, idx: PageNo) -> Page {
        // println!("Reading page {}", idx.borrow());
        let mut p = Page::default();

        let content = p.content.as_mut_slice();

        let mut file = self.file_at_page_readonly(idx.borrow().clone());
        file.read_exact(content).unwrap();

        p
    }

    pub fn get_and_decode<P: Borrow<P> + DeserializeOwned, PageNo: Borrow<u32>>(
        &self,
        idx: PageNo,
    ) -> P {
        let p = self.get(idx);
        let reader = BufReader::new(p.borrow().content.as_slice());
        let mut deserializer = serde_json::Deserializer::from_reader(reader);
        P::deserialize(&mut deserializer).unwrap()
    }

    pub fn set<P: Borrow<Page>, PageNo: Borrow<u32>>(&mut self, idx: PageNo, page: P) {
        // println!("Writing page {}", idx.borrow());
        let mut file = self.file_at_page_write(idx.borrow().clone());
        file.write_all(&page.borrow().content).unwrap();
    }

    pub fn encode_and_set<P: Borrow<P> + Serialize, PageNo: Borrow<u32>>(
        &mut self,
        idx: PageNo,
        v: P,
    ) -> Result<(), EncodingError> {
        let mut page = Page::default();
        let result = serde_json::to_writer(page.content.as_mut_slice(), v.borrow());

        match result {
            Err(e) => match e.classify() {
                serde_json::error::Category::Io => {
                    return Err(EncodingError::NotEnoughSpaceInPage);
                }
                serde_json::error::Category::Syntax => todo!(),
                serde_json::error::Category::Data => todo!(),
                serde_json::error::Category::Eof => todo!(),
            },
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

    pub fn get_root_page(&self, root_name: &str) -> Option<u32> {
        let zero = self.get_zero_page()?;

        zero.root_pages.get(&root_name.to_string()).copied()
    }

    pub fn set_root_page(&mut self, root_name: &str, idx: u32) {
        let mut zero = self.get_zero_page().unwrap();

        zero.root_pages.insert(root_name.to_string(), idx);

        self.set_zero_page(zero);
    }

    pub fn debug(&self, message: &str) {
        for i in 0..self.get_file_size_pages() {
            let page: serde_json::Value = self.get_and_decode(i);

            println!("{message}: Page {i} : {page}");
        }
    }

    pub fn get_tree_names(&self) -> Vec<String> {
        let zp = self.get_zero_page();
        if zp.is_none() {
            return vec![];
        }
        let zp = zp.unwrap();

        zp.root_pages.keys().cloned().collect()
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

        page_one_content.content[0] = 0;
        page_one_content.content[10] = 0;

        page_two_content.content[0] = 0;
        page_two_content.content[20] = 0;

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
}
