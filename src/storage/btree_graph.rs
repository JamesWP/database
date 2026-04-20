use std::fmt::Result;
use std::fmt::Write;

use super::btree::{decode_integer_key, decode_u64_key, BTree};
use super::cell_reader::CellReader;
use super::node::NodePage;
use super::node_page_store::NodePageStore;
use super::page_id::PageId;
use crate::engine::scalarvalue::ScalarValue;

const CATALOG_ROOT: u32 = 1;

enum TreeKind<'a> {
    Table,
    Index { col_names: &'a [String] },
}

fn node_name_str(page_idx: u32) -> String {
    format!("node_{page_idx}")
}

fn format_scalar(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Integer(i) => i.to_string(),
        ScalarValue::Floating(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::String(s) => format!("\"{}\"", s),
        ScalarValue::Blob(b) => format!("Blob({})", b.len()),
        ScalarValue::Null => "NULL".to_string(),
    }
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('<', "\\<")
        .replace('>', "\\>")
        .replace('{', "\\{")
        .replace('}', "\\}")
        .replace('|', "\\|")
}

fn format_table_key(bytes: &[u8]) -> String {
    if bytes.len() == 8 {
        decode_u64_key(bytes).to_string()
    } else {
        format!(
            "0x{}",
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        )
    }
}

fn format_index_key(bytes: &[u8], col_names: &[String]) -> String {
    let mut pos = 0;
    let mut parts: Vec<String> = Vec::new();

    for name in col_names {
        if pos >= bytes.len() {
            break;
        }
        let tag = bytes[pos];
        pos += 1;
        let val = match tag {
            0x00 => "NULL".to_string(),
            0x01 if pos + 8 <= bytes.len() => {
                let v = decode_integer_key(&bytes[pos..pos + 8]);
                pos += 8;
                format!("{name}={v}")
            }
            0x02 if pos + 8 <= bytes.len() => {
                let bits = u64::from_be_bytes(bytes[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let bits = if bits >> 63 == 1 {
                    bits ^ 0x8000_0000_0000_0000
                } else {
                    bits ^ 0xFFFF_FFFF_FFFF_FFFF
                };
                format!("{name}={}", f64::from_bits(bits))
            }
            0x03 => {
                let end = bytes[pos..]
                    .iter()
                    .position(|&b| b == 0x00)
                    .unwrap_or(bytes.len() - pos);
                let s = std::str::from_utf8(&bytes[pos..pos + end]).unwrap_or("?");
                pos += end + 1;
                format!("{name}={s:?}")
            }
            _ => format!("{name}=?"),
        };
        parts.push(val);
    }

    // trailing 8 bytes = rowid
    let rowid = if pos + 8 <= bytes.len() {
        decode_u64_key(&bytes[pos..pos + 8])
    } else {
        0
    };
    parts.push(format!("rowid={rowid}"));
    parts.join(", ")
}

fn write_leaf_node<W: Write>(
    output: &mut W,
    store: &mut NodePageStore,
    page_idx: u32,
    kind: &TreeKind<'_>,
) -> Result {
    // First pass: collect key strings and overflow flags from the leaf page.
    struct CellInfo {
        key_display: String,
        overflow_page: Option<u32>,
    }

    let cell_infos: Vec<CellInfo> = {
        let page = store.read(PageId(page_idx)).expect("read leaf page");
        let NodePage::Leaf(ref leaf) = *page else {
            return Ok(());
        };
        (0..leaf.num_items())
            .filter_map(|i| {
                let cell = leaf.get_item_at_index(i)?;
                let key_display = match kind {
                    TreeKind::Table => format_table_key(cell.key()),
                    TreeKind::Index { col_names } => format_index_key(cell.key(), col_names),
                };
                let overflow_page = cell.continuation();
                Some(CellInfo {
                    key_display,
                    overflow_page,
                })
            })
            .collect()
    }; // page borrow ends here

    // Second pass: decode cell values (CellReader eagerly loads everything).
    let mut cells: Vec<String> = Vec::new();
    let mut overflow_stubs: Vec<(usize, u32)> = Vec::new();

    for (i, info) in cell_infos.iter().enumerate() {
        let val_display = if info.overflow_page.is_some() {
            overflow_stubs.push((i, info.overflow_page.unwrap()));
            "(overflow)".to_string()
        } else if let Some(mut reader) = CellReader::new(store, page_idx, i) {
            let values = reader.decode_as_array();
            values
                .iter()
                .map(|v| escape_label(&format_scalar(v)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            String::new()
        };

        cells.push(format!(
            "<c{i}>{}: {}",
            escape_label(&info.key_display),
            val_display
        ));
    }

    let label = cells.join("|");
    writeln!(output, "\t{} [label=\"{label}\"];", node_name_str(page_idx))?;

    // Overflow chains: follow continuation pointers until the end.
    for (i, first_overflow_page) in overflow_stubs {
        let mut prev_name = format!("{}:c{i}", node_name_str(page_idx));
        let mut cur_page = first_overflow_page;
        let mut hop = 0usize;

        loop {
            let overflow_name = format!("node_{page_idx}_c{i}_overflow{hop}");
            let (continuation, bytes) = {
                let page = store.read(PageId(cur_page)).expect("read overflow page");
                let NodePage::OverflowPage(ref overflow) = *page else {
                    break;
                };
                (overflow.continuation(), overflow.value().len())
            }; // page borrow ends here
            writeln!(
                output,
                "\t{overflow_name} [label=\"overflow\\n(page {cur_page}, {bytes}B)\" shape=ellipse style=dashed];"
            )?;
            writeln!(output, "\t{prev_name} -> {overflow_name} [style=dashed];")?;

            match continuation {
                None => break,
                Some(next_page) => {
                    prev_name = overflow_name;
                    cur_page = next_page;
                    hop += 1;
                }
            }
        }
    }

    Ok(())
}

fn write_interior_node<W: Write>(
    output: &mut W,
    store: &mut NodePageStore,
    page_idx: u32,
) -> Result {
    let (label, edges): (String, Vec<(usize, u32)>) = {
        let page = store.read(PageId(page_idx)).expect("read interior page");
        let NodePage::Interior(ref interior) = *page else {
            return Ok(());
        };

        let mut label_parts: Vec<String> = vec!["<e_0>.".to_string()];
        for edge_idx in 1..interior.num_edges() {
            let key = interior.get_key_by_index(edge_idx - 1);
            let key_display = escape_label(&format!("{key:?}"));
            label_parts.push(format!("{key_display}|<e_{edge_idx}>."));
        }

        let edges: Vec<(usize, u32)> = (0..interior.num_edges())
            .map(|i| (i, interior.get_child_page_by_index(i)))
            .collect();

        (label_parts.join("|"), edges)
    }; // page borrow ends here

    writeln!(output, "\t{} [label=\"{label}\"];", node_name_str(page_idx))?;

    for (edge_idx, child) in edges {
        writeln!(
            output,
            "\t{}:e_{edge_idx} -> {};",
            node_name_str(page_idx),
            node_name_str(child)
        )?;
    }

    Ok(())
}

fn write_subgraph<W: Write>(
    output: &mut W,
    btree: &BTree,
    root: u32,
    label: &str,
    kind: TreeKind<'_>,
) -> Result {
    writeln!(output, "\tsubgraph cluster_{root} {{")?;
    writeln!(output, "\t\tlabel=\"{}\";", escape_label(label))?;

    let mut store = btree.store_mut();

    // DFS walk from root
    let mut stack = vec![root];
    let mut visited = std::collections::HashSet::new();

    while let Some(page_idx) = stack.pop() {
        if !visited.insert(page_idx) {
            continue;
        }

        // Peek at page type and extract child list if interior.
        // Inner block ensures the read borrow is dropped before write_leaf_node
        // or write_interior_node re-borrows via the same store reference.
        enum PageKind {
            Leaf,
            Interior(Vec<u32>),
            Overflow,
        }
        let kind_hint: PageKind = {
            let page = store.read(PageId(page_idx)).expect("read page in DFS");
            match page {
                NodePage::Leaf(_) => PageKind::Leaf,
                NodePage::Interior(ref interior) => {
                    let children: Vec<u32> = (0..interior.num_edges())
                        .map(|i| interior.get_child_page_by_index(i))
                        .collect();
                    PageKind::Interior(children)
                }
                NodePage::OverflowPage(_) => PageKind::Overflow,
            }
        }; // page borrow ends here

        match kind_hint {
            PageKind::Leaf => {
                write_leaf_node(output, &mut *store, page_idx, &kind)?;
            }
            PageKind::Interior(children) => {
                write_interior_node(output, &mut *store, page_idx)?;
                for child in children {
                    stack.push(child);
                }
            }
            PageKind::Overflow => {
                // overflow pages are linked from leaf stubs; skip in DFS
            }
        }
    }

    writeln!(output, "\t}}")?;
    Ok(())
}

pub fn dump<W: Write>(output: &mut W, btree: &BTree) -> Result {
    writeln!(output, "digraph Database {{")?;
    writeln!(output, "\tnode [shape=record fontname=\"monospace\"]")?;
    writeln!(output, "\trankdir=\"LR\";")?;

    // Catalog tree
    write_subgraph(
        output,
        btree,
        CATALOG_ROOT,
        "db_schema (catalog)",
        TreeKind::Table,
    )?;

    // User tables and indexes (skip root pages already rendered)
    let cache = btree.catalog();
    for (name, info) in &cache.parsed_tables {
        if info.rootpage == CATALOG_ROOT {
            continue;
        }
        write_subgraph(output, btree, info.rootpage, name, TreeKind::Table)?;
    }
    for (_tbl_name, indexes) in &cache.indexes {
        for idx in indexes {
            write_subgraph(
                output,
                btree,
                idx.rootpage,
                &format!("idx: {}", idx.index_name),
                TreeKind::Index {
                    col_names: &idx.column_names,
                },
            )?;
        }
    }

    writeln!(output, "}}")?;
    Ok(())
}
