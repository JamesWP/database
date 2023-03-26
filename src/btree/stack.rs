use crate::{node::{self}, pager};

use super::Tuple;

type NodePage = node::NodePage<u64, Tuple>;
type LeafNodePage = node::LeafNodePage<u64, Tuple>;

/// a pair of the page number and the index in that page 
type StackItem = (u32, usize); 
type Stack = Vec<StackItem>;

pub struct PartialSearchStack<'a> {
    pager: &'a mut pager::Pager,
    stack: Stack,

    // the "top" of the stack
    next: u32
}

pub struct SearchStack<'a> {
    pager: &'a mut pager::Pager,
    stack: Stack,

    /// The location in the node pointed to by the stack which we were looking for
    top: StackItem,
}

pub enum PushResult<'a> {
    /// The push resulted in finding a new child node and is now also pointing to that
    Grew(PartialSearchStack<'a>),

    /// The push resulted in finding the location we were searching for, we now have the entire
    /// path to the node we were looking for
    Done(SearchStack<'a>)
}

impl<'a> PartialSearchStack<'a> {
    pub fn new(pager: &'a mut pager::Pager, root_page_number: u32) -> PartialSearchStack {
        PartialSearchStack {
            pager,
            stack: Default::default(),
            next: root_page_number,
        }
    }

    pub fn top(&self) -> NodePage {
        self.pager.get_and_decode(self.top_page_idx())
    }

    pub fn top_page_idx(&self) -> u32 {
        self.next
    }

    pub fn push(self, idx: u32) -> PushResult<'a> {
        todo!()
    }
}

impl SearchStack<'_> {
    pub fn insert(self, key: u64, value: Tuple) {
        // Insert value into the leaf(node) at the top of the stack

        let (page_idx, item_idx) = self.top;
        let mut page: NodePage = self.pager.get_and_decode(page_idx);

        page.insert_item_at_index(item_idx, key, value);

        // TODO: handle encode faling due to lack of space
        self.pager.encode_and_set(page_idx, page);

        // loop until all splits are resolved
        // if we have no split -> return
        // if we have a split (key, value, )
    }
}
