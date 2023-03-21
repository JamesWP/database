mod pager;

mod btree {
    use crate::pager;

    pub struct BTree {
        pager: pager::Pager,
    }
}

mod database {
    use crate::btree;

    struct Database {
        btree: btree::BTree,
    }
}

fn main() {
    println!("Hello, world!");
}
