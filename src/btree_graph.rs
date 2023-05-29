use std::fmt::Result;
use std::fmt::Write;

use crate::node;
use crate::node::{InteriorNodePage, LeafNodePage, NodePage};
use crate::pager::Pager;

/*
Example of output text:

digraph G {
    node[
        shape=record
    ]

    // Interior
    root[label="<e1>.|key=4|<e2>."]

    root:e1 -> child1;

    // Leaf
    child1[label="<v1>1|<v2>2|<v3>3"]

    child1:v1 -> value1;

    value1[label="A"]

    child1:v2 -> value2;

    value2[label="B"]

    child1:v3 -> value3;

    value3[label="C"]

}

*/

fn node_name<W: Write>(output: &mut W, page_idx: u32) -> Result {
    write!(output, "node_{}", page_idx)?;

    Ok(())
}

fn interor_edge<W: Write>(output: &mut W, page_idx: u32, edge_idx: usize) -> Result {
    write!(output, "node_{}:", page_idx)?;
    interior_tag(output, edge_idx)?;

    Ok(())
}

fn value_edge<W: Write>(output: &mut W, page_idx: u32, value_idx: usize) -> Result {
    write!(output, "node_{}:", page_idx)?;
    value_tag(output, value_idx)?;

    Ok(())
}

fn value_node<W: Write>(output: &mut W, page_idx: u32, value_idx: usize) -> Result {
    write!(output, "value_{}_", page_idx)?;
    value_tag(output, value_idx)?;

    Ok(())
}

fn value_tag<W: Write>(output: &mut W, value_idx: usize) -> Result {
    write!(output, "v_{}", value_idx)?;

    Ok(())
}

fn interior_tag<W: Write>(output: &mut W, value_idx: usize) -> Result {
    write!(output, "e_{}", value_idx)?;

    Ok(())
}

fn quote(message: &str) -> String {
    let mut s = String::new();

    write!(s, "\"").unwrap();
    message
        .chars()
        .take(20)
        .map(|c| match c {
            '"' => '_',
            ch => ch,
        })
        .for_each(|ch| s.write_char(ch).unwrap());
    if message.chars().skip(20).next().is_some() {
        write!(s, "...").unwrap();
    }
    write!(s, "\"").unwrap();

    s
}

// Shamelessly copied from itertools
fn join<I: Iterator<Item = T>, T: std::fmt::Display>(iter: &mut I, sep: &str) -> String {
    match iter.next() {
        None => String::new(),
        Some(first_elt) => {
            // estimate lower bound of capacity needed
            let (lower, _) = iter.size_hint();
            let mut result = String::with_capacity(sep.len() * lower);
            write!(&mut result, "{}", first_elt).unwrap();
            iter.for_each(|elt| {
                result.push_str(sep);
                write!(&mut result, "{}", elt).unwrap();
            });
            result
        }
    }
}

fn to_json_string(value: &Vec<serde_json::Value>) -> String {
    serde_json::Value::Array(value.clone()).to_string()
}

pub fn dump<W: Write>(output: &mut W, pager: &Pager) -> Result {
    writeln!(output, "digraph Database {{")?;

    writeln!(output, "\tnode [ shape=record ]")?;
    writeln!(output, "\trankdir=\"LR\";")?;

    for page_idx in 1..pager.get_file_size_pages() {
        let page: NodePage = pager.get_and_decode(page_idx);

        match page {
            node::NodePage::Leaf(l) => {
                write!(output, "\t")?;
                node_name(output, page_idx)?;
                let mut label = (0..l.num_items()).map(|cell_idx| {
                    let cell = l.get_item_at_index(cell_idx).unwrap();
                    format!("<v_{}>{:?}", cell_idx, cell.key())
                });
                let label = join(&mut label, "|");
                let quoted_label = &label;
                writeln!(output, "[label=\"{quoted_label}\"]")?;

                for cell_idx in 0..l.num_items() {
                    write!(output, "\t")?;
                    value_edge(output, page_idx, cell_idx)?;
                    write!(output, " -> ")?;
                    value_node(output, page_idx, cell_idx)?;
                    writeln!(output, ";")?;

                    write!(output, "\t")?;
                    value_node(output, page_idx, cell_idx)?;
                    let value = &l.get_item_at_index(cell_idx).unwrap().value();
                    writeln!(output, "[label={:?}]", value)?;
                }
            }
            node::NodePage::Interior(i) => {
                write!(output, "\t")?;
                node_name(output, page_idx)?;
                let mut label = (1..i.num_edges()).map(|edge_index| {
                    // Key | edge
                    let key = i.get_key_by_index(edge_index - 1);
                    format!("key={key:?}|<e_{edge_index}>.")
                });

                let label = join(&mut label, "|");

                let label = format!("<e_0>.| {label}");
                let quoted_label = &label;
                writeln!(output, "[label=\"{quoted_label}\"]")?;

                for edge_index in 0..i.num_edges() {
                    write!(output, "\t")?;
                    interor_edge(output, page_idx, edge_index)?;
                    write!(output, " -> ")?;
                    let child_page_idx = i.get_child_page_by_index(edge_index);
                    node_name(output, child_page_idx)?;
                    writeln!(output, ";")?;
                }
            }
        }

        writeln!(output)?;
    }

    writeln!(output, "}}")?;

    Ok(())
}
