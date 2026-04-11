use crate::storage::node::NodePage;

use super::{
    node::{self, InteriorNodePage, LeafNodePage},
    node_page_store::NodePageStore,
    page_id::PageId,
};

#[derive(Debug)]
pub enum VerifyError {
    KeyOutOfOrder,
    Imbalance,
}

impl From<node::VerifyError> for VerifyError {
    fn from(value: node::VerifyError) -> Self {
        match value {
            node::VerifyError::KeyOutOfOrder => Self::KeyOutOfOrder,
        }
    }
}

fn verify_leaf(_store: &mut NodePageStore, leaf: LeafNodePage) -> Result<usize, VerifyError> {
    // Check each leaf page has keys (unless its a root node)
    assert!(leaf.num_items() > 0);

    // Check the keys in each leaf page are in order
    leaf.verify_key_ordering()?;

    Ok(0)
}

fn verify_interior(
    store: &mut NodePageStore,
    interior: InteriorNodePage,
) -> Result<usize, VerifyError> {
    // if interior page contains edges to leaves, all edges must be leaves
    // if interior page contains edges to interior nodes, each interior node must have leaves at the same level
    // Check all interior node's keys are in order
    interior.verify_key_ordering()?;

    // Check all interior nodes are half full of entries ???
    // They should have at least two edges
    assert!(interior.num_edges() > 1);
    assert_eq!(interior.num_edges() - 1, interior.num_keys());

    // Check all interior node's child page's keys are within bounds
    for edge in 0..interior.num_edges() - 1 {
        let child_page_idx = interior.get_child_page_by_index(edge);
        let child_page: NodePage = {
            let n = store
                .read(PageId(child_page_idx))
                .map_err(|_| VerifyError::Imbalance)?;
            n.clone()
        };

        let edge_key = interior.get_key_by_index(edge);
        let smallest_key = child_page.smallest_key();
        let largest_key = child_page.largest_key();

        assert!(smallest_key <= largest_key);
        assert!(largest_key <= edge_key);
    }

    let mut edge_levels = vec![];

    for edge in 0..interior.num_edges() {
        let edge_idx = interior.get_child_page_by_index(edge);
        let edge_node: NodePage = {
            let n = store
                .read(PageId(edge_idx))
                .map_err(|_| VerifyError::Imbalance)?;
            n.clone()
        };
        let level = verify_node(store, edge_node)?;
        edge_levels.push(level);
    }

    let first_level = edge_levels.first().unwrap().clone();

    if edge_levels
        .into_iter()
        .skip(1)
        .filter(|l| *l != first_level)
        .next()
        .is_some()
    {
        // found at least one edge with a different level to the first edge
        return Err(VerifyError::Imbalance);
    }

    Ok(first_level)
}

fn verify_node(store: &mut NodePageStore, node: NodePage) -> Result<usize, VerifyError> {
    match node {
        NodePage::Leaf(l) => verify_leaf(store, l),
        NodePage::Interior(i) => verify_interior(store, i),
        NodePage::OverflowPage(_) => Ok(1000),
    }
}

pub fn verify(store: &mut NodePageStore, root_page_idx: u32) -> Result<(), VerifyError> {
    let root_page: NodePage = {
        let n = store
            .read(PageId(root_page_idx))
            .map_err(|_| VerifyError::Imbalance)?;
        n.clone()
    };

    match root_page {
        NodePage::Leaf(l) => {
            // we dont need to do the other validation if the leaf is the root node
            l.verify_key_ordering()?;
        }
        NodePage::Interior(i) => {
            verify_interior(store, i)?;
        }
        NodePage::OverflowPage(_) => {
            panic!()
        }
    };

    Ok(())
}
