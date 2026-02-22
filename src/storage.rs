mod cell;
mod cell_reader;
mod node;
mod pager;

/// Btree module heavily inspired by the fantastic article: https://cglab.ca/~abeinges/blah/rust-btree-case/
///
/// And the btree structures described in: https://www.sqlite.org/fileformat.html
mod btree;

mod btree_graph;
mod btree_verify;

pub use btree::decode_integer_key;
pub use btree::decode_u64_key;
pub use btree::encode_index_value;
pub use btree::encode_integer_key;
pub use btree::encode_u64_key;
pub use btree::BTree;
pub use btree::CursorHandle;
pub use btree::IndexInfo;
pub use cell_reader::CellReader;
