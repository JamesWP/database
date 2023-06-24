use std::{
    borrow::BorrowMut,
    cell::{Ref, RefCell},
    cmp::{min, max},
    io::{Read, Write}, ops::ControlFlow,
};

use owning_ref::OwningHandle;
use rand::{
    distributions::uniform::{UniformInt, UniformSampler},
    Rng,
};

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

mod database {
    use crate::btree;
}

mod frontend;

enum State {
    None,
    Open(Box<btree::BTree>),
    Cursor(OwningHandle<Box<btree::BTree>, Box<btree::Cursor<&'static mut pager::Pager>>>),
}

pub(crate) fn main() {
    let mut args = std::env::args().skip(1);

    let db_name = args.next().expect("first arg should be database name");

    let db_path = std::path::Path::new(&db_name);

    if db_path.exists() {
        println!("Path {db_path:?} exists. opening");
        assert!(
            db_path.is_file(),
            "Path {db_path:?} is not a file directory"
        );
    } else {
        println!("Path {db_path:?} does not exist. creating");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&db_path)
            .expect("can create database file");
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
                    let cursor = unsafe { btree_ptr.as_mut().unwrap().open_readwrite(&tree_name) };
                    let cursor: Option<btree::Cursor<&'static mut pager::Pager>> =
                        unsafe { std::mem::transmute(cursor) };
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
                    State::Open(_) => {
                        println!("Open a table before printing");
                        continue;
                    }
                    State::None => {
                        println!("Open a database before printing");
                        continue;
                    }
                };

                cursor.first();

                loop {
                    let entry = cursor.get_entry();
                    if let ControlFlow::Break(_) = print_value(entry) {
                        break;
                    }

                    cursor.next();
                }
            }
            ["first"] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };

                cursor.first();
            }
            ["next"] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };

                cursor.next();
            }
            ["prev"] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };

                cursor.prev();
            }
            ["find", key] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };
                let key = u64::from_str_radix(*key, 10).unwrap();

                cursor.find(key);
            }
            ["print"] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };

                print_value(cursor.get_entry());
            }
            ["insert", key, rest @ ..] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
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
            ["random", "insert", count, max_size] => {
                let cursor = match &mut state {
                    State::None => {
                        println!("No database open");
                        continue;
                    }
                    State::Open(database) => {
                        println!("No cursor open");
                        continue;
                    }
                    State::Cursor(cursor) => cursor.borrow_mut(),
                };

                let count = u64::from_str_radix(*count, 10).unwrap();
                let max_size = u64::from_str_radix(*&max_size, 10).unwrap();

                let max_size = max(11usize, max_size as usize);
                let count = max(11usize, count as usize);

                for _ in 0..count {
                    let mut rng = rand::thread_rng();
                    let size = rng.sample(rand::distributions::Uniform::new(10, max_size));
                    let mut bytes = Vec::with_capacity(size);
                    for _ in 0..size {
                        bytes.push(0);
                    }
                    rng.fill(bytes.as_mut_slice());

                    let key =
                        rng.sample(rand::distributions::Uniform::new(1 << 10, 1 << 32 as u64));

                    cursor.insert(key, bytes);
                }

                println!("Inserted {count} items with a random size up to {max_size}");
            }
            ["dump", path] => {
                let path = std::path::Path::new(*path);

                let result = match &state {
                    State::None => panic!(),
                    State::Cursor(_) => {
                        println!("Close open cursor before dumping");
                        continue;
                    }
                    State::Open(db) => db.dump_to_file(&path),
                };

                match result {
                    Err(e) => {
                        println!("Error dumping to {:?}", &path);
                        println!("Error: {}", e);
                        continue;
                    }
                    Ok(_) => {
                        println!("Dumped graph to {:?}", &path);
                        continue;
                    }
                }
            }
            ["verify"] => {
                let result = match &state {
                    State::None => panic!(),
                    State::Cursor(c) => c.verify(),
                    State::Open(db) => db.verify(),
                };

                match result {
                    Err(e) => {
                        println!("Verify error {:?}", &e);
                        continue;
                    }
                    Ok(_) => {
                        println!("Verify Success!");
                        continue;
                    }
                }
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

fn print_value(entry: Option<cell_reader::CellReader<'_>>) -> ControlFlow<()> {
    if entry.is_none() {
        println!("Cursor is complete");
        return ControlFlow::Break(());
    }
    let mut entry = entry.unwrap();
    let key = entry.key();
    let mut value_buf = Vec::new();
    let value_size = entry.read_to_end(&mut value_buf);
    let str_value = String::from_utf8(value_buf);
    match (value_size, str_value) {
        (Ok(len), Ok(str_value)) if len < 80 => {
            println!("Entry: key={key}, len={len} value={str_value}")
        }
        (Ok(len), Ok(_)) => {
            println!("Entry: key={key}, len={len} value=<redacted>")
        }
        (Ok(len), Err(_)) => {
            println!("Entry: key={key}, len={len} value=<unable to decode utf8>")
        }
        (Err(_), _) => println!("Entry: key={key}, value=<unable to read value>"),
    };

    ControlFlow::Continue(())
}
