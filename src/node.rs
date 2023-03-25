use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum NodePage<K, V> {
    Leaf(LeafNodePage<K, V>),
    Interior(InteriorNodePage<K>),
}

impl<K, V> NodePage<K, V> {
    pub fn search(self, k: &K)-> SearchResult<K, V> {
        match self {
            NodePage::Leaf(l) => l.search(k),
            NodePage::Interior(i) => todo!(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LeafNodePage<K, V> {
    keys: Vec<(K, V)>,
}

impl<K, V> Default for LeafNodePage<K, V> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
        }
    }
}

pub enum SearchResult<K, V> {
    /// The value was found at the given index of the given leaf node
    Found(LeafNodePage<K, V>, usize),

    /// The element wasn't found, but if it is anywhere 
    /// then it must be in the child node identified by the given page number
    GoDown(u32),
}

impl<K, V> LeafNodePage<K, V> {
    pub fn search(self, k: &K) -> SearchResult<K, V> {
        todo!()
    }

    pub fn set_item_at_index(&mut self, index: usize, value: V) {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage<K> {
    keys: Vec<K>,
    edges: Vec<u32>,
    // TODO: create interior node
}
