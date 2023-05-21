mod pager;
mod node;

/// Btree module heavily inspired by the fantastic article: https://cglab.ca/~abeinges/blah/rust-btree-case/
/// 
/// And the btree structures described in: https://www.sqlite.org/fileformat.html
mod btree;

mod btree_verify;
mod btree_graph;

mod node_new;

mod database {
    use crate::btree;

    struct Database {
        btree: btree::BTree,
    }
}

fn main() {
    println!("Hello, world!");
}
