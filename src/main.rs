mod pager;

mod btree {
    use crate::pager;

    pub struct ReadonlyCursor<'a> {
        btree: &'a BTree,

        /// key for the item pointed to by the cursor
        key: Option<u64>,  
    }

    impl<'a> ReadonlyCursor<'a> {
        /// Move the cursor to point at the first row in the btree
        /// This may result in the cursor not pointing to a row if there is no
        /// first row to point to
        fn first(&mut self) {
            todo!()
        }

        /// Move the cursor to point at the last row in the btree
        /// This may result in the cursor not pointing to a row if there is no
        /// last row to point to
        fn last(&mut self) {
            todo!()
        }

        /// Move the cursor to point at the row in the btree identified by the given key
        /// This may result in the cursor not pointing to a row if there is no
        /// row found with that key to point to
        fn find(&mut self, key: u64) {
            todo!()
        }

        /// get the value at the specified column index from the row pointed to by the cursor,
        /// or None if the cursor is not pointing to a row
        fn column(&self, col_idx: u32) -> Option<serde_json::Value> {
            todo!()
        }

        /// Move the cursor to point at the next item in the btree
        fn next(&mut self) {
            todo!()
        }

        /// Move the cursor to point at the next item in the btree
        fn prev(&mut self) {
            todo!()
        }
    }

    pub struct WriteonlyCursor<'a> {
        btree: &'a mut BTree,
    }

    impl<'a> WriteonlyCursor<'a> {
        fn insert(&mut self, key: u64, value: Vec<serde_json::Value>) {
            todo!()
        }
    }

    pub struct BTree {
        pager: pager::Pager,
    }

    impl BTree {
        fn open_readonly<'a>(&'a self, tree_name: &str) -> Option<ReadonlyCursor<'a>> {
            let idx = self.pager.get_root_page(tree_name)?;

            Some(ReadonlyCursor { btree: self, key:None })
        }
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
