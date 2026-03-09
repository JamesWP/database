use std::fmt::Result;
use std::fmt::Write;

use super::btree::{decode_integer_key, decode_u64_key, BTree};
use super::cell_reader::CellReader;
use super::node::NodePage;

enum TreeKind<'a> {
    Table,
    Index { col_names: &'a [String] },
}

fn node_name_str(page_idx: u32) -> String {
    format!("node_{page_idx}")
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
    btree: &BTree,
    page_idx: u32,
    kind: &TreeKind<'_>,
) -> Result {
    // First pass: collect keys and overflow flags (while pager is borrowed)
    struct CellInfo {
        key_display: String,
        overflow_page: Option<u32>,
    }

    let cell_infos: Vec<CellInfo> = {
        let pager = btree.pager.borrow();
        let page = pager.get_and_decode(page_idx);
        let NodePage::Leaf(leaf) = page else {
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
    };

    // Second pass: decode values via CellReader (re-borrows pager each time)
    let mut cells: Vec<String> = Vec::new();
    let mut overflow_stubs: Vec<(usize, u32)> = Vec::new();

    for (i, info) in cell_infos.iter().enumerate() {
        let val_display = if info.overflow_page.is_some() {
            overflow_stubs.push((i, info.overflow_page.unwrap()));
            "(overflow)".to_string()
        } else {
            let pager = btree.pager.borrow();
            if let Some(mut reader) = CellReader::new(&pager, page_idx, i) {
                let values = reader.decode_as_array();
                values
                    .iter()
                    .map(|v| escape_label(&format!("{v}")))
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                String::new()
            }
        };

        cells.push(format!(
            "<c{i}>{}: {}",
            escape_label(&info.key_display),
            val_display
        ));
    }

    let label = cells.join("|");
    writeln!(output, "\t{} [label=\"{label}\"];", node_name_str(page_idx))?;

    // Overflow stubs
    for (i, overflow_page) in overflow_stubs {
        let overflow_name = format!("node_{page_idx}_c{i}_overflow");
        writeln!(
            output,
            "\t{overflow_name} [label=\"overflow\\n(page {overflow_page})\" shape=ellipse style=dashed];"
        )?;
        writeln!(
            output,
            "\t{}:c{i} -> {overflow_name} [style=dashed];",
            node_name_str(page_idx)
        )?;
    }

    Ok(())
}

fn write_interior_node<W: Write>(output: &mut W, btree: &BTree, page_idx: u32) -> Result {
    let pager = btree.pager.borrow();
    let page = pager.get_and_decode(page_idx);
    let NodePage::Interior(interior) = page else {
        return Ok(());
    };

    let mut label_parts: Vec<String> = vec!["<e_0>.".to_string()];
    for edge_idx in 1..interior.num_edges() {
        let key = interior.get_key_by_index(edge_idx - 1);
        let key_display = escape_label(&format!("{key:?}"));
        label_parts.push(format!("{key_display}|<e_{edge_idx}>."));
    }

    let label = label_parts.join("|");
    writeln!(output, "\t{} [label=\"{label}\"];", node_name_str(page_idx))?;

    for edge_idx in 0..interior.num_edges() {
        let child = interior.get_child_page_by_index(edge_idx);
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

    // DFS walk from root
    let mut stack = vec![root];
    let mut visited = std::collections::HashSet::new();

    while let Some(page_idx) = stack.pop() {
        if !visited.insert(page_idx) {
            continue;
        }

        let pager = btree.pager.borrow();
        let page = pager.get_and_decode(page_idx);

        match page {
            NodePage::Leaf(_) => {
                drop(pager);
                write_leaf_node(output, btree, page_idx, &kind)?;
            }
            NodePage::Interior(interior) => {
                let num_edges = interior.num_edges();
                let mut children = Vec::new();
                for i in 0..num_edges {
                    children.push(interior.get_child_page_by_index(i));
                }
                drop(pager);
                write_interior_node(output, btree, page_idx)?;
                for child in children {
                    stack.push(child);
                }
            }
            NodePage::OverflowPage(_) => {
                // overflow pages linked from leaf stubs; skip in DFS
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
    if let Some(root) = btree.schema_root_page() {
        write_subgraph(output, btree, root, "db_schema (catalog)", TreeKind::Table)?;
    }

    // User tables and indexes (skip root pages already rendered)
    let schema_root = btree.schema_root_page();
    for (kind, name, tbl_name, rootpage, _sql) in btree.scan_schema_entries() {
        if Some(rootpage) == schema_root {
            continue;
        }
        match kind.as_str() {
            "table" => {
                write_subgraph(output, btree, rootpage, &name, TreeKind::Table)?;
            }
            "index" => {
                let col_names: Vec<String> = btree
                    .lookup_indexes_for_table(&tbl_name)
                    .into_iter()
                    .find(|i| i.index_name == name)
                    .map(|i| i.column_names)
                    .unwrap_or_default();
                write_subgraph(
                    output,
                    btree,
                    rootpage,
                    &format!("idx: {name}"),
                    TreeKind::Index {
                        col_names: &col_names,
                    },
                )?;
            }
            _ => {}
        }
    }

    writeln!(output, "}}")?;
    Ok(())
}
