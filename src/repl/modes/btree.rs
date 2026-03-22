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
                let root_page = shared.btree.btree_mut().create_tree();
                shared
                    .btree
                    .insert_entry("table", name, name, root_page, &sql);
                CommandResult::Message(format!("Created table '{}' at page {}", name, root_page))
            }

            ["tables"] | ["list", "tables"] => {
                let schema_root: u32 = 1;

                let mut cursor = shared.btree.btree_mut().open(schema_root);
                let mut c = cursor.open_readonly();

                println!("Tables:");
                c.first();
                loop {
                    let entry = c.get_entry();
                    match entry {
                        None => break,
                        Some(mut reader) => {
                            let values = reader.decode_as_array();
                            // Schema: [type, name, tbl_name, rootpage, sql]
                            if values.len() >= 5 {
                                if let Some(obj_type) = values[0].as_str() {
                                    if obj_type == "table" {
                                        if let (Some(name), Some(rootpage)) =
                                            (values[1].as_str(), values[3].as_u64())
                                        {
                                            println!("  {} (root page: {})", name, rootpage);
                                        }
                                    }
                                }
                            }
                            c.next();
                        }
                    }
                }
                CommandResult::Ok
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
                        let handle = shared.btree.btree_mut().open(root_page);
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
                    cursor.handle.open_readonly().find_u64(key);
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
                        .insert_u64(key, value.into_bytes());
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

                        cursor.handle.open_readwrite().insert_u64(key, bytes);
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

                // Scan the catalog to collect all table and index names and root pages.
                // db_schema is included because it has a self-referencing row.
                let entries = {
                    let mut cursor = shared.btree.btree_mut().open(schema_root);
                    let mut c = cursor.open_readonly();
                    c.first();
                    let mut entries = Vec::new();
                    loop {
                        match c.get_entry() {
                            None => break,
                            Some(mut reader) => {
                                let row = reader.decode_as_array();
                                if row.len() >= 5 {
                                    let kind = row[0].as_str().unwrap_or("");
                                    if kind == "table" || kind == "index" {
                                        let name = row[1].as_str().unwrap_or("").to_string();
                                        let rootpage = row[3].as_u64().unwrap() as u32;
                                        entries.push((name, kind.to_string(), rootpage));
                                    }
                                }
                            }
                        }
                        c.next();
                    }
                    entries
                };

                if entries.is_empty() {
                    return CommandResult::Message("No tables or indexes found".to_string());
                }

                // Verify each B-tree
                let mut failures = Vec::new();
                for (name, kind, rootpage) in &entries {
                    let mut handle = shared.btree.btree_mut().open(*rootpage);
                    let result = handle.open_readonly().verify();
                    if let Err(e) = result {
                        failures.push(format!("  {} ({}): {:?}", name, kind, e));
                    }
                }
                if failures.is_empty() {
                    CommandResult::Message(format!(
                        "All {} tables/indexes verified OK",
                        entries.len()
                    ))
                } else {
                    CommandResult::Error(format!(
                        "Verification failed for {} B-tree(s):\n{}",
                        failures.len(),
                        failures.join("\n")
                    ))
                }
            }

            ["inspect", "page", page_num] => {
                let page_num: u32 = match page_num.parse() {
                    Ok(n) => n,
                    Err(_) => return CommandResult::Error("Invalid page number".to_string()),
                };

                match shared.btree.btree().inspect_page(page_num) {
                    Ok(_) => CommandResult::Ok,
                    Err(e) => CommandResult::Error(e),
                }
            }

            ["inspect", "pages"] | ["inspect", "all"] => {
                let file_size = shared.btree.btree().file_size_pages();

                for page_num in 0..file_size {
                    if let Err(e) = shared.btree.btree().inspect_page(page_num) {
                        return CommandResult::Error(e);
                    }
                    println!();
                }
                CommandResult::Ok
            }

            ["dump", path] => {
                if self.cursor.is_some() {
                    return CommandResult::Error("Close cursor before dumping".to_string());
                }

                let path = std::path::Path::new(*path);
                match shared.btree.btree().dump_to_file(path) {
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
    tables / list tables      List all tables in the database
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
    inspect page <n>          Show raw CBOR structure of page n
    inspect pages / all       Show raw CBOR structure of all pages
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
            let key_hex = entry
                .key()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("");
            let mut value_buf = Vec::new();
            let value_size = entry.read_to_end(&mut value_buf);

            if value_size.is_err() {
                println!("Entry: key={}, value=<unable to read value>", key_hex);
                return ControlFlow::Continue(());
            }

            let len = value_size.unwrap();

            // Try CBOR decoding first (as Vec<ScalarValue>)
            if let Ok(scalar_values) = ciborium::de::from_reader::<
                Vec<database::engine::scalarvalue::ScalarValue>,
                _,
            >(&value_buf[..])
            {
                let display = format!("{:?}", scalar_values);
                if display.len() < 80 {
                    println!("Entry: key={}, len={} value={}", key_hex, len, display);
                } else {
                    println!(
                        "Entry: key={}, len={} value=<redacted {} items>",
                        key_hex,
                        len,
                        scalar_values.len()
                    );
                }
                return ControlFlow::Continue(());
            }

            // Try UTF-8 decoding
            if let Ok(str_value) = String::from_utf8(value_buf.clone()) {
                if str_value.len() < 80 {
                    println!("Entry: key={}, len={} value={}", key_hex, len, str_value);
                } else {
                    println!("Entry: key={}, len={} value=<redacted text>", key_hex, len);
                }
                return ControlFlow::Continue(());
            }

            // Fall back to hex preview for binary data
            let hex_preview: String = value_buf
                .iter()
                .take(16)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            let suffix = if value_buf.len() > 16 { "..." } else { "" };
            println!(
                "Entry: key={}, len={} value=<binary: {}{}>",
                key_hex, len, hex_preview, suffix
            );

            ControlFlow::Continue(())
        }
    }
}
