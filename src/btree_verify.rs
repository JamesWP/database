use crate::node;
use crate::pager::Pager;

use crate::node::{InteriorNodePage, LeafNodePage, NodePage};

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

fn verify_leaf(pager: &Pager, leaf: LeafNodePage) -> Result<usize, VerifyError> {
    // Check each leaf page has keys (unless its a root node)
    assert!(leaf.num_items() > 0);

    // Check the keys in each leaf page are in order
    leaf.verify_key_ordering()?;

    Ok(0)
}

fn verify_interior(pager: &Pager, interior: InteriorNodePage) -> Result<usize, VerifyError> {
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
        let child_page: NodePage = pager.get_and_decode(child_page_idx);

        let edge_key = interior.get_key_by_index(edge);
        let smallest_key = child_page.smallest_key();
        let largest_key = child_page.largest_key();

        assert!(smallest_key <= largest_key);
        assert!(largest_key <= edge_key);
    }

    let mut edge_levels = vec![];

    for edge in 0..interior.num_edges() {
        let edge_idx = interior.get_child_page_by_index(edge);
        let edge: NodePage = pager.get_and_decode(edge_idx);
        let level = verify_node(pager, edge)?;
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

fn verify_node(pager: &Pager, node: NodePage) -> Result<usize, VerifyError> {
    match node {
        NodePage::Leaf(l) => verify_leaf(pager, l),
        NodePage::Interior(i) => verify_interior(pager, i),
        NodePage::OverflowPage(_) => Ok(1000),
    }
}

pub fn verify(pager: &Pager, tree_name: &str) -> Result<(), VerifyError> {
    let root_page_idx = pager.get_root_page(tree_name).unwrap();
    let root_page: NodePage = pager.get_and_decode(root_page_idx);

    match root_page {
        NodePage::Leaf(l) => {
            // we dont need to do the other validation if the leaf is the root node
            l.verify_key_ordering()?;
        }
        NodePage::Interior(i) => {
            verify_interior(pager, i)?;
        }
        NodePage::OverflowPage(_) => {
            panic!()
        }
    };

    Ok(())
}

pub fn verify_all_trees(pager: &Pager) -> Result<(), VerifyError> {
    let tree_names = pager.get_tree_names();
    for tree_name in tree_names {
        verify(pager, &tree_name)?;
    }
    Ok(())
}
