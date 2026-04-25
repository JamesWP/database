use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

#[wasm_bindgen_test]
fn test_create_table_and_insert() {
    let mut db = database::wasm::Database::new();
    let r = db.execute("CREATE TABLE t (id INTEGER, val TEXT)");
    assert!(r.is_ok());
    let r = db.execute("INSERT INTO t VALUES (1, 'hello')");
    assert!(r.is_ok());
}

#[wasm_bindgen_test]
fn test_query_returns_rows() {
    let mut db = database::wasm::Database::new();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (7)").unwrap();
    let rows = db.query("SELECT x FROM t").unwrap();
    let arr = js_sys::Array::from(&rows);
    assert_eq!(arr.length(), 1);
}
