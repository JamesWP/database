use std::cmp::Ordering::{Equal, Greater, Less};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum NodePage<K, V> {
    Leaf(LeafNodePage<K, V>),
    Interior(InteriorNodePage<K>),
}

impl<K, V> NodePage<K, V> 
where K: Ord
{
    pub fn search(self, k: &K) -> SearchResult {
        match self {
            NodePage::Leaf(l) => l.search(k),
            NodePage::Interior(i) => todo!(),
        }
    }

    pub fn insert_item_at_index(&mut self, item_idx: usize, key: K, value: V) {
        match self {
            NodePage::Leaf(l) => {
                l.insert_item_at_index(item_idx, key, value);
            }
            NodePage::Interior(_) => todo!(),
        };
    }

    pub fn set_item_at_index(&mut self, item_idx: usize, value: V) {
        match self {
            NodePage::Leaf(l) => {
                l.set_item_at_index(item_idx, value);
            }
            NodePage::Interior(_) => todo!(),
        };
    }
}

#[derive(Serialize, Deserialize)]
pub struct LeafNodePage<K, V> {
    cells: Vec<(K, V)>,
}

impl<K, V> Default for LeafNodePage<K, V> {
    fn default() -> Self {
        Self {
            cells: Default::default(),
        }
    }
}

pub enum SearchResult {
    /// The value was found at the given index of the given leaf node
    Found(usize),
    /// The value is not present in the leaf node, but if it were it should be at this index
    NotPresent(usize),
    /// The element wasn't found, but if it is anywhere
    /// then it must be in the child node identified by the given page number
    GoDown(u32),
}

pub enum InsertionResult<K, V> {
    /// The insertion is done, and there was space enough to fit the item in the page
    Fit,

    /// The insertion was not possible, it resulted in the node splitting.
    /// The split resulted in two nodes, this one (self) and another in this result.
    /// The smallest key in the other node is also returned.
    Split(K, NodePage<K, V>),
}

impl<K, V> LeafNodePage<K, V>
where
    K: Ord,
{
    pub fn search(self, search_key: &K) -> SearchResult {
        // Simple linear search through the page.
        for (index, (key, _value)) in self.cells.iter().enumerate() {
            match key.cmp(search_key) {
                Less => { return SearchResult::NotPresent(index)},
                Equal => { return SearchResult::Found(index) },
                Greater => {}, // Continue the search
            }
        }
        
        SearchResult::NotPresent(self.cells.len())
    }

    pub fn set_item_at_index(&mut self, index: usize, value: V) {
        todo!()
    }

    pub fn insert_item_at_index(
        &mut self,
        index: usize,
        key: K,
        value: V,
    ) -> InsertionResult<K, V> {
        // put item into leaf at given index.

        self.cells.insert(index, (key, value));

        InsertionResult::Fit

        // Check if this page is overfull..
        // We might have to check about the insertion in the caller...
        // TODO: handle splitting if full.
    }

    pub fn get_item_at_index(&self, entry_index: usize) -> Option<&(K, V)> {
        self.cells.get(entry_index)
    }
}

#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage<K> {
    keys: Vec<K>,
    edges: Vec<u32>,
    // TODO: create interior node
}
