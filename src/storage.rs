mod catalog_cache;
mod cell;
mod cell_reader;
mod error;
mod node;
mod node_page_store;
mod page_id;
mod page_storage;
mod pager;

/// Btree module heavily inspired by the fantastic article: https://cglab.ca/~abeinges/blah/rust-btree-case/
///
/// And the btree structures described in: https://www.sqlite.org/fileformat.html
mod btree;

mod btree_graph;
mod btree_verify;

#[cfg(not(target_arch = "wasm32"))]
pub use page_storage::FilePageStorage;
pub use page_storage::MemoryPageStorage;
pub use page_storage::PageStorage;
pub use page_storage::PAGE_SIZE;

pub use btree::decode_integer_key;
pub use btree::decode_u64_key;
pub use btree::encode_index_value;
pub use btree::encode_integer_key;
pub use btree::encode_u64_key;
pub use btree::BTree;
pub use btree::CursorHandle;
pub use catalog_cache::CatalogSnapshot;
pub use catalog_cache::IndexInfo;
pub use cell_reader::CellReader;
pub use error::Error as StorageError;
pub use node::NodePage;
pub use node_page_store::NodePageStore;
pub use page_id::PageId;
