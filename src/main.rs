mod pager;
mod node;
mod btree;

mod database {
    use crate::btree;

    struct Database {
        btree: btree::BTree,
    }
}

fn main() {
    println!("Hello, world!");
}
