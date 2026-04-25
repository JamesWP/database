use js_sys::Array;
use wasm_bindgen::prelude::*;

use crate::db::{execute, ExecuteResult};
use crate::engine::scalarvalue::ScalarValue;
use crate::storage::BTree;

#[wasm_bindgen]
pub struct Database {
    btree: BTree,
}

#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database {
            btree: BTree::new_in_memory(),
        }
    }

    /// Execute a DDL or DML statement. Returns a status string.
    pub fn execute(&mut self, sql: &str) -> Result<String, JsValue> {
        execute(sql, &mut self.btree)
            .map(format_result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Execute a SELECT. Returns a JS Array of row arrays.
    pub fn query(&mut self, sql: &str) -> Result<JsValue, JsValue> {
        let result =
            execute(sql, &mut self.btree).map_err(|e| JsValue::from_str(&e.to_string()))?;
        match result {
            ExecuteResult::Query(mut q) => {
                let arr = Array::new();
                while let Some(row) = q.next() {
                    let js_row = Array::new();
                    for val in &row {
                        js_row.push(&scalar_to_js(val));
                    }
                    arr.push(&js_row);
                }
                Ok(arr.into())
            }
            _ => Ok(Array::new().into()),
        }
    }
}

fn format_result(r: ExecuteResult) -> String {
    match r {
        ExecuteResult::CreateTable { table_name } => format!("Table '{}' created", table_name),
        ExecuteResult::CreateIndex { index_name } => format!("Index '{}' created", index_name),
        ExecuteResult::DropTable { table_name } => format!("Table '{}' dropped", table_name),
        ExecuteResult::Query(_) | ExecuteResult::Explain(_) => "ok".to_string(),
    }
}

fn scalar_to_js(v: &ScalarValue) -> JsValue {
    match v {
        ScalarValue::Integer(i) => JsValue::from(*i),
        ScalarValue::Floating(f) => JsValue::from(*f),
        ScalarValue::Boolean(b) => JsValue::from(*b),
        ScalarValue::String(s) => JsValue::from_str(s),
        ScalarValue::Blob(b) => {
            let arr = js_sys::Uint8Array::new_with_length(b.len() as u32);
            arr.copy_from(b);
            arr.into()
        }
        ScalarValue::Null => JsValue::null(),
    }
}
