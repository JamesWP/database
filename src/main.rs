use std::{io::{Write, Read}, cell::{RefCell, Ref}, borrow::BorrowMut};

use owning_ref::{OwningHandle};

mod node;
mod pager;
mod cell;
mod cell_reader;

/// Btree module heavily inspired by the fantastic article: https://cglab.ca/~abeinges/blah/rust-btree-case/
///
/// And the btree structures described in: https://www.sqlite.org/fileformat.html
mod btree;

mod btree_graph;
mod btree_verify;

mod database {
    use crate::btree;

}

enum State {
    None,
    Open(Box<btree::BTree>),
    Cursor(OwningHandle<Box<btree::BTree>, Box<btree::Cursor<&'static mut pager::Pager>>>)
}

pub(crate) fn main() {
    let mut args = std::env::args().skip(1);

    let db_name = args.next().expect("first arg should be database name");

    let db_path = std::path::Path::new(&db_name);

    if db_path.exists() {
        println!("Path {db_path:?} exists. opening");
        assert!(db_path.is_file(), "Path {db_path:?} is not a file directory");
    } else {
        println!("Path {db_path:?} does not exist. creating");
        std::fs::OpenOptions::new().write(true).create(true).open(&db_path).expect("can create database file");
    }
    
    let db_path = db_path.canonicalize().unwrap();

    let btree = Box::new(btree::BTree::new(db_path.to_str().unwrap()));
    let mut state = State::Open(btree);

    loop {
        print!("> ");
        std::io::stdout().lock().flush().unwrap();
        let mut line = String::new();
        let length = std::io::stdin().read_line(&mut line).unwrap();
        println!();
        if length == 0 {
            break;
        }

        let line = line.to_lowercase();
        let line = line.trim();
        let line: Vec<_> = line.split_ascii_whitespace().collect();

        match line.as_slice() {
            ["create", "table", rest @ ..] => {
                let tree_name = rest.join(" ");
                println!("creating tree '{tree_name}'");
                match &mut state {
                    State::Open(btree) => btree.create_tree(&tree_name),
                    _ => {
                        println!("btree already opened");
                        continue;
                    }
                };
            }
            ["read", "table", rest @ ..] => {
                let tree_name = rest.join(" ");
                println!("read table '{tree_name}'");

                let btree = match state {
                    State::Open(btree) => btree,
                    _ => {
                        println!("Table already open");
                        continue;
                    }
                };

                let open_cursor = |btree_ptr: *const btree::BTree| {
                    let btree_ptr: *mut btree::BTree = unsafe { std::mem::transmute(btree_ptr) };
                    let cursor = unsafe{ btree_ptr.as_mut().unwrap().open_readwrite(&tree_name)};
                    let cursor: Option<btree::Cursor<&'static mut pager::Pager>> = unsafe {std::mem::transmute(cursor)};
                    let cursor = match cursor {
                        Some(cursor) => {
                            println!("Obtained a readonly cursor for {tree_name}");
                            cursor
                        }
                        None => {
                            panic!("Unable to open {tree_name}");
                        }
                    };
                    Box::new(cursor)
                };

                state = State::Cursor(OwningHandle::new_with_fn(btree, open_cursor));
            }
            ["print", "data"] => {
                let cursor = match &mut state {
                    State::Cursor(handle) => handle.borrow_mut(),
                    _ => panic!()
                };

                cursor.first();

                loop {
                    let entry = cursor.get_entry();
                    if entry.is_none() {
                        println!("Cursor is complete");
                        break;
                    }

                    let mut entry = entry.unwrap();
                    let key = entry.key();
                    let mut value_buf = String::new();
                    let value = entry.read_to_string(&mut value_buf);

                    match value {
                        Ok(len) => println!("Entry: key={key}, value={value_buf}"),
                        Err(e) => println!("Entry: key={key}, value=<unable to read value>"),
                    };

                    cursor.next();
                }
            }
            ["insert", key,  rest@..] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    },
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };
                let key: u64 = u64::from_str_radix(*key, 10).unwrap();
                let value = rest.join(" ");
                cursor.insert(key, value.into_bytes());
            }
            ["close"] => {
                state = match state {
                    State::None => {
                        println!("No open database");
                        State::None
                    }
                    State::Open(btree) => {
                        println!("No open cursors");
                        State::Open(btree)
                    }
                    State::Cursor(cursor) => {
                        println!("Closed open cursor");
                        State::Open(OwningHandle::into_owner(cursor))
                    }
                }
            }
            _ => {
                let line = line.join(" ");
                println!("Command not understood '{line}'");
            }
        }
    }
}
