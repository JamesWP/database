use js_sys::Array;
use wasm_bindgen::prelude::*;

use crate::db::{execute, ExecuteResult};
use crate::engine::scalarvalue::ScalarValue;
use crate::storage::{BTree, PageStorage, PAGE_SIZE};

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

    /// Create a Database backed by a custom JS storage provider.
    ///
    /// `provider` must implement the `PageStorageProvider` interface:
    /// - `pageCount(): number`
    /// - `setPageCount(n: number): void`
    /// - `readPage(n: number): Uint8Array`   — exactly 4096 bytes
    /// - `writePage(n: number, data: Uint8Array): void`
    /// - `flush(): void`
    ///
    /// All methods are called synchronously. For async backends (S3, IndexedDB),
    /// implement the provider in a Worker and use `Atomics.wait()` to block.
    #[wasm_bindgen(js_name = withStorage)]
    pub fn with_storage(provider: JsValue) -> Result<Database, JsValue> {
        let obj = js_sys::Object::try_from(&provider)
            .cloned()
            .ok_or_else(|| JsValue::from_str("storage provider must be an object"))?;
        let storage = JsPageStorage { provider: obj };
        Ok(Database {
            btree: BTree::with_storage(storage),
        })
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

// ── JsPageStorage ─────────────────────────────────────────────────────────────

struct JsPageStorage {
    provider: js_sys::Object,
}

impl PageStorage for JsPageStorage {
    fn page_count(&self) -> u32 {
        call_method_0(&self.provider, "pageCount")
            .as_f64()
            .unwrap_or(0.0) as u32
    }

    fn set_page_count(&mut self, count: u32) {
        call_method_1(&self.provider, "setPageCount", JsValue::from(count));
    }

    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE] {
        let val = call_method_1(&self.provider, "readPage", JsValue::from(page_no));
        let arr = js_sys::Uint8Array::from(val);
        let mut bytes = [0u8; PAGE_SIZE];
        arr.copy_to(&mut bytes);
        bytes
    }

    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        let arr = js_sys::Uint8Array::new_with_length(PAGE_SIZE as u32);
        arr.copy_from(&bytes[..]);
        call_method_2(
            &self.provider,
            "writePage",
            JsValue::from(page_no),
            arr.into(),
        );
    }

    fn flush(&mut self) -> std::io::Result<()> {
        call_method_0(&self.provider, "flush");
        Ok(())
    }
}

fn call_method_0(obj: &js_sys::Object, method: &str) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call0(obj)
        .expect("call failed")
}

fn call_method_1(obj: &js_sys::Object, method: &str, arg: JsValue) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call1(obj, &arg)
        .expect("call failed")
}

fn call_method_2(obj: &js_sys::Object, method: &str, a: JsValue, b: JsValue) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call2(obj, &a, &b)
        .expect("call failed")
}

// ── helpers ───────────────────────────────────────────────────────────────────

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
