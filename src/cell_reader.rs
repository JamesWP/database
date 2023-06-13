use std::ops::Range;
use std::pin::Pin;
use std::ptr::slice_from_raw_parts_mut;

use crate::cell::{Cell, Key, ValueRef};
use crate::node::{LeafNodePage, NodePage};
use crate::pager::Pager;

// TODO: refactor to make this safer
//       unsafe pointer dereference
//       node member contains a box nodepage which we point into with buf_ptr
pub struct CellReader<'a> {
    pager: &'a Pager,
    key: Key,
    continuation: Option<u32>,

    // Unsafe pair, buf points into node, whenever we change node we must also update buf
    node: Box<NodePage>,
    buf: &'a [u8],
}

impl<'a> std::io::Read for CellReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.buf.read(buf)?;

        if bytes_read != 0 {
            return Ok(bytes_read);
        }

        match self.continuation {
            None => Ok(0),
            Some(continuation) => {
                self.node = Box::new(self.pager.get_and_decode(continuation));
                let overflow_page = match self.node.as_ref() {
                    NodePage::OverflowPage(p) => p,
                    _ => panic!(),
                };

                // TODO: factor the unsafe into seperate struct
                let value = overflow_page.value();
                self.buf = unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) };
                self.continuation = overflow_page.continuation();

                self.buf.read(buf)
            }
        }
    }
}

impl<'a> CellReader<'a> {
    pub fn new(pager: &'a Pager, leaf_page_idx: u32, cell_idx: usize) -> Option<CellReader<'a>> {
        let node: Box<NodePage> = Box::new(pager.get_and_decode(leaf_page_idx));

        let leaf_page = node
            .leaf()
            .expect("Values are always supposed to be in leaf pages");

        let cell = leaf_page.get_item_at_index(cell_idx)?;
        let key = cell.key();
        let continuation = cell.continuation();
        let value = cell.value();

        // TODO: factor the unsafe into seperate struct
        let buf = unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) };

        Some(CellReader {
            pager,
            node,
            buf,
            key,
            continuation,
        })
    }

    pub fn key(&self) -> Key {
        self.key
    }
}
