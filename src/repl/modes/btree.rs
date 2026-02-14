use std::cmp::max;
use std::io::Read;
use std::ops::ControlFlow;

use rand::Rng;

use crate::repl::{CommandResult, Mode, ModeId, SharedState};
use database::frontend::ast;
use database::frontend::parse;
use database::storage::{CellReader, CursorHandle};

/// BTree mode state - cursor is created/dropped as part of mode state
#[derive(Debug)]
pub struct BTreeMode {
    cursor: Option<CursorState>,
}

#[derive(Debug)]
struct CursorState {
    table_name: String,
    handle: CursorHandle,
}

impl BTreeMode {
    pub fn new(_shared: &mut SharedState) -> Self {
        BTreeMode { cursor: None }
    }
}

impl Mode for BTreeMode {
    fn id(&self) -> ModeId {
        ModeId::BTree
    }

    fn prompt(&self) -> String {
        match &self.cursor {
            None => "btree> ".to_string(),
            Some(state) => format!("btree:{}> ", state.table_name),
        }
    }

    fn execute(&mut self, tokens: &[&str], shared: &mut SharedState) -> CommandResult {
        match tokens {
            // Table management
            ["create", "table", rest @ ..] => {
                let sql = format!("CREATE TABLE {}", rest.join(" "));
                let stmt = match parse(&sql) {
                    Ok(ast::Statement::CreateTable(ct)) => ct,
                    Ok(_) => {
                        return CommandResult::Error("Expected CREATE TABLE statement".to_string())
                    }
                    Err(e) => return CommandResult::Error(format!("Parse error: {:?}", e)),
                };
                let name = &stmt.table_name;
                if shared.btree.lookup_table(name).is_some() {
                    return CommandResult::Error(format!("Table '{}' already exists", name));
                }
                let root_page = shared.btree.create_tree();
                let key = name.bytes().fold(0u64, |acc, b: u8| {
                    acc.wrapping_mul(31).wrapping_add(b as u64)
                });
                shared
                    .btree
                    .insert_schema_entry(key, "table", name, name, root_page, &sql);
                CommandResult::Message(format!("Created table '{}' at page {}", name, root_page))
            }

            // Cursor operations
            ["open", rest @ ..] | ["read", "table", rest @ ..] => {
                let name = rest.join(" ");
                if name.is_empty() {
                    return CommandResult::Error("Usage: open <table>".to_string());
                }

                if self.cursor.is_some() {
                    return CommandResult::Error(
                        "Cursor already open. Use 'close' first.".to_string(),
                    );
                }

                match shared.btree.lookup_table(&name) {
                    Some((root_page, _)) => {
                        let handle = shared.btree.open(root_page);
                        self.cursor = Some(CursorState {
                            table_name: name.clone(),
                            handle,
                        });
                        CommandResult::Message(format!("Opened cursor on '{}'", name))
                    }
                    None => CommandResult::Error(format!("Table '{}' not found", name)),
                }
            }

            ["close"] => match self.cursor.take() {
                None => CommandResult::Message("No cursor open".to_string()),
                Some(state) => {
                    CommandResult::Message(format!("Closed cursor on '{}'", state.table_name))
                }
            },

            // Navigation
            ["first"] => self.with_cursor(|cursor| {
                cursor.handle.open_readonly().first();
                CommandResult::Ok
            }),

            ["next"] => self.with_cursor(|cursor| {
                cursor.handle.open_readonly().next();
                CommandResult::Ok
            }),

            ["prev"] => self.with_cursor(|cursor| {
                cursor.handle.open_readonly().prev();
                CommandResult::Ok
            }),

            ["find", key] => {
                let key: u64 = match key.parse() {
                    Ok(k) => k,
                    Err(_) => return CommandResult::Error("Invalid key (must be u64)".to_string()),
                };
                self.with_cursor(|cursor| {
                    cursor.handle.open_readonly().find(key);
                    CommandResult::Ok
                })
            }

            // Read operations
            ["print"] => self.with_cursor(|cursor| {
                let mut c = cursor.handle.open_readonly();
                let entry = c.get_entry();
                let _ = print_value(entry);
                CommandResult::Ok
            }),

            ["print", "data"] | ["scan"] => self.with_cursor(|cursor| {
                let mut c = cursor.handle.open_readonly();
                c.first();
                loop {
                    let entry = c.get_entry();
                    if let ControlFlow::Break(_) = print_value(entry) {
                        break;
                    }
                    c.next();
                }
                CommandResult::Ok
            }),

            // Write operations
            ["insert", key, rest @ ..] => {
                let key: u64 = match key.parse() {
                    Ok(k) => k,
                    Err(_) => return CommandResult::Error("Invalid key (must be u64)".to_string()),
                };
                let value = rest.join(" ");
                self.with_cursor(|cursor| {
                    cursor
                        .handle
                        .open_readwrite()
                        .insert(key, value.into_bytes());
                    CommandResult::Message(format!("Inserted key {}", key))
                })
            }

            ["random", "insert", count, max_size] => {
                let count: u64 = match count.parse() {
                    Ok(c) => c,
                    Err(_) => {
                        return CommandResult::Error("Invalid count (must be u64)".to_string())
                    }
                };
                let max_size: u64 = match max_size.parse() {
                    Ok(s) => s,
                    Err(_) => {
                        return CommandResult::Error("Invalid max_size (must be u64)".to_string())
                    }
                };

                let max_size = max(11usize, max_size as usize);
                let count = max(11usize, count as usize);

                self.with_cursor(|cursor| {
                    for _ in 0..count {
                        let mut rng = rand::thread_rng();
                        let size = rng.sample(rand::distributions::Uniform::new(10, max_size));
                        let mut bytes = vec![0u8; size];
                        rng.fill(bytes.as_mut_slice());

                        let key =
                            rng.sample(rand::distributions::Uniform::new(1 << 10, 1u64 << 32));

                        cursor.handle.open_readwrite().insert(key, bytes);
                    }
                    CommandResult::Message(format!(
                        "Inserted {} items with random size up to {}",
                        count, max_size
                    ))
                })
            }

            // Debug operations
            ["verify"] => match &mut self.cursor {
                None => {
                    CommandResult::Error("No cursor open. Use 'open <table>' first.".to_string())
                }
                Some(cursor) => match cursor.handle.open_readonly().verify() {
                    Ok(_) => CommandResult::Message("Verify success!".to_string()),
                    Err(e) => CommandResult::Error(format!("Verify failed: {:?}", e)),
                },
            },

            ["verify", "all"] => {
                // Open db_schema like any other table via lookup_table
                let (schema_root, _) = match shared.btree.lookup_table("db_schema") {
                    Some(r) => r,
                    None => return CommandResult::Error("No schema table found".to_string()),
                };

                // Scan the catalog to collect all table names and root pages.
                // db_schema is included because it has a self-referencing row.
                let tables = {
                    let mut cursor = shared.btree.open(schema_root);
                    let mut c = cursor.open_readonly();
                    c.first();
                    let mut tables = Vec::new();
                    loop {
                        match c.get_entry() {
                            None => break,
                            Some(mut reader) => {
                                let row = reader.decode_as_json_array();
                                if row.len() >= 5 && row[0].as_str() == Some("table") {
                                    let name = row[1].as_str().unwrap_or("").to_string();
                                    let rootpage = row[3].as_u64().unwrap() as u32;
                                    tables.push((name, rootpage));
                                }
                            }
                        }
                        c.next();
                    }
                    tables
                };

                if tables.is_empty() {
                    return CommandResult::Message("No tables found".to_string());
                }

                // Verify each table's B-tree
                let mut failures = Vec::new();
                for (name, rootpage) in &tables {
                    let mut handle = shared.btree.open(*rootpage);
                    let result = handle.open_readonly().verify();
                    if let Err(e) = result {
                        failures.push(format!("  {}: {:?}", name, e));
                    }
                }
                if failures.is_empty() {
                    CommandResult::Message(format!("All {} tables verified OK", tables.len()))
                } else {
                    CommandResult::Error(format!(
                        "Verification failed for {} table(s):\n{}",
                        failures.len(),
                        failures.join("\n")
                    ))
                }
            }

            ["dump", path] => {
                if self.cursor.is_some() {
                    return CommandResult::Error("Close cursor before dumping".to_string());
                }

                let path = std::path::Path::new(*path);
                match shared.btree.dump_to_file(path) {
                    Ok(_) => CommandResult::Message(format!("Dumped graph to {:?}", path)),
                    Err(e) => CommandResult::Error(format!("Error dumping: {}", e)),
                }
            }

            _ => CommandResult::NotHandled,
        }
    }

    fn help(&self) -> String {
        r#"BTree mode commands:
  Table management:
    create table <name> (<cols>)  Create a table with schema (SQL syntax)
    open <name>               Open a cursor on a table
    read table <name>         Alias for open
    close                     Close the current cursor

  Navigation (requires open cursor):
    first                     Move to first entry
    next                      Move to next entry
    prev                      Move to previous entry
    find <key>                Find entry by key

  Read operations:
    print                     Print current entry
    print data / scan         Print all entries

  Write operations (requires open cursor):
    insert <key> <value>      Insert a key-value pair
    random insert <n> <size>  Insert n random entries

  Debug:
    verify                    Verify current table's B-tree integrity
    verify all                Verify all tables in the database
    dump <path>               Export B-tree as graphviz dot file"#
            .to_string()
    }
}

impl BTreeMode {
    fn with_cursor<F>(&mut self, f: F) -> CommandResult
    where
        F: FnOnce(&mut CursorState) -> CommandResult,
    {
        match &mut self.cursor {
            None => CommandResult::Error("No cursor open. Use 'open <table>' first.".to_string()),
            Some(cursor) => f(cursor),
        }
    }
}

fn print_value(entry: Option<CellReader<'_>>) -> ControlFlow<()> {
    match entry {
        None => {
            println!("Cursor is complete");
            ControlFlow::Break(())
        }
        Some(mut entry) => {
            let key = entry.key();
            let mut value_buf = Vec::new();
            let value_size = entry.read_to_end(&mut value_buf);
            let str_value = String::from_utf8(value_buf);
            match (value_size, str_value) {
                (Ok(len), Ok(str_value)) if len < 80 => {
                    println!("Entry: key={}, len={} value={}", key, len, str_value)
                }
                (Ok(len), Ok(_)) => {
                    println!("Entry: key={}, len={} value=<redacted>", key, len)
                }
                (Ok(len), Err(_)) => {
                    println!(
                        "Entry: key={}, len={} value=<unable to decode utf8>",
                        key, len
                    )
                }
                (Err(_), _) => println!("Entry: key={}, value=<unable to read value>", key),
            }
            ControlFlow::Continue(())
        }
    }
}
