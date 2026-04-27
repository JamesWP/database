/// Exploration: current vs compact CBOR encoding for ScalarValue.
///
/// Run with:
///   cargo run -p database --example cbor_sizes
///
/// Phase BJ sizing study: compare the current serde-derive map encoding
/// against two compact alternatives.
use database::engine::scalarvalue::ScalarValue;

fn encode_current(v: &ScalarValue) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(v, &mut buf).unwrap();
    buf
}

fn encode_row_current(row: &[ScalarValue]) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(row, &mut buf).unwrap();
    buf
}

// --- Option A: integer-tagged array [tag, payload] ---
// Tags:  0=Null  1=Integer  2=Floating  3=Boolean  4=String  5=Blob

fn encode_tagged(v: &ScalarValue) -> Vec<u8> {
    use ciborium::value::Value;
    let cbor = match v {
        ScalarValue::Null => Value::Array(vec![Value::Integer(0.into())]),
        ScalarValue::Integer(n) => Value::Array(vec![
            Value::Integer(1.into()),
            Value::Integer(ciborium::value::Integer::try_from(*n).unwrap()),
        ]),
        ScalarValue::Floating(f) => Value::Array(vec![Value::Integer(2.into()), Value::Float(*f)]),
        ScalarValue::Boolean(b) => Value::Array(vec![Value::Integer(3.into()), Value::Bool(*b)]),
        ScalarValue::String(s) => {
            Value::Array(vec![Value::Integer(4.into()), Value::Text(s.clone())])
        }
        ScalarValue::Blob(b) => {
            Value::Array(vec![Value::Integer(5.into()), Value::Bytes(b.clone())])
        }
    };
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&cbor, &mut buf).unwrap();
    buf
}

fn encode_row_tagged(row: &[ScalarValue]) -> Vec<u8> {
    let elems: Vec<ciborium::value::Value> = row
        .iter()
        .map(|v| {
            use ciborium::value::Value;
            match v {
                ScalarValue::Null => Value::Array(vec![Value::Integer(0.into())]),
                ScalarValue::Integer(n) => Value::Array(vec![
                    Value::Integer(1.into()),
                    Value::Integer(ciborium::value::Integer::try_from(*n).unwrap()),
                ]),
                ScalarValue::Floating(f) => {
                    Value::Array(vec![Value::Integer(2.into()), Value::Float(*f)])
                }
                ScalarValue::Boolean(b) => {
                    Value::Array(vec![Value::Integer(3.into()), Value::Bool(*b)])
                }
                ScalarValue::String(s) => {
                    Value::Array(vec![Value::Integer(4.into()), Value::Text(s.clone())])
                }
                ScalarValue::Blob(b) => {
                    Value::Array(vec![Value::Integer(5.into()), Value::Bytes(b.clone())])
                }
            }
        })
        .collect();
    let outer = ciborium::value::Value::Array(elems);
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&outer, &mut buf).unwrap();
    buf
}

// --- Option B: native CBOR types (no wrapper) ---
// Integer   -> CBOR uint / nint
// Floating  -> CBOR float (double-precision fb)
// Boolean   -> CBOR true/false (f5/f4)
// Null      -> CBOR null (f6)
// String    -> CBOR text string
// Blob      -> CBOR byte string

fn encode_native(v: &ScalarValue) -> Vec<u8> {
    use ciborium::value::Value;
    let cbor = match v {
        ScalarValue::Null => Value::Null,
        ScalarValue::Integer(n) => Value::Integer(ciborium::value::Integer::try_from(*n).unwrap()),
        ScalarValue::Floating(f) => Value::Float(*f),
        ScalarValue::Boolean(b) => Value::Bool(*b),
        ScalarValue::String(s) => Value::Text(s.clone()),
        ScalarValue::Blob(b) => Value::Bytes(b.clone()),
    };
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&cbor, &mut buf).unwrap();
    buf
}

fn encode_row_native(row: &[ScalarValue]) -> Vec<u8> {
    use ciborium::value::Value;
    let elems: Vec<Value> = row
        .iter()
        .map(|v| match v {
            ScalarValue::Null => Value::Null,
            ScalarValue::Integer(n) => {
                Value::Integer(ciborium::value::Integer::try_from(*n).unwrap())
            }
            ScalarValue::Floating(f) => Value::Float(*f),
            ScalarValue::Boolean(b) => Value::Bool(*b),
            ScalarValue::String(s) => Value::Text(s.clone()),
            ScalarValue::Blob(b) => Value::Bytes(b.clone()),
        })
        .collect();
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&Value::Array(elems), &mut buf).unwrap();
    buf
}

fn main() {
    let cases: Vec<(&str, ScalarValue)> = vec![
        ("Null", ScalarValue::Null),
        ("Integer(0)", ScalarValue::Integer(0)),
        ("Integer(1)", ScalarValue::Integer(1)),
        ("Integer(23)", ScalarValue::Integer(23)),
        ("Integer(24)", ScalarValue::Integer(24)),
        ("Integer(255)", ScalarValue::Integer(255)),
        ("Integer(256)", ScalarValue::Integer(256)),
        ("Integer(65535)", ScalarValue::Integer(65535)),
        ("Integer(65536)", ScalarValue::Integer(65536)),
        ("Integer(i64::MAX)", ScalarValue::Integer(i64::MAX)),
        ("Integer(-1)", ScalarValue::Integer(-1)),
        ("Integer(-128)", ScalarValue::Integer(-128)),
        ("Floating(0.0)", ScalarValue::Floating(0.0)),
        ("Floating(3.14)", ScalarValue::Floating(3.14)),
        ("Boolean(true)", ScalarValue::Boolean(true)),
        ("Boolean(false)", ScalarValue::Boolean(false)),
        ("String(\"\")", ScalarValue::String(String::new())),
        (
            "String(\"hello\")",
            ScalarValue::String("hello".to_string()),
        ),
        (
            "String(\"hello world\")",
            ScalarValue::String("hello world".to_string()),
        ),
        ("Blob([])", ScalarValue::Blob(vec![])),
        ("Blob([1,2,3])", ScalarValue::Blob(vec![1, 2, 3])),
    ];

    println!("=== Per-value encoding size comparison ===\n");
    println!(
        "{:<25} {:>7}  {:>7}  {:>7}",
        "Value", "current", "tagged", "native"
    );
    println!("{}", "-".repeat(55));
    for (label, val) in &cases {
        let cur = encode_current(val);
        let tag = encode_tagged(val);
        let nat = encode_native(val);
        println!(
            "{:<25} {:>7}  {:>7}  {:>7}",
            label,
            cur.len(),
            tag.len(),
            nat.len()
        );
    }

    let rows: Vec<(&str, Vec<ScalarValue>)> = vec![
        (
            "[1, \"alice\", 30]",
            vec![
                ScalarValue::Integer(1),
                ScalarValue::String("alice".to_string()),
                ScalarValue::Integer(30),
            ],
        ),
        (
            "[NULL, \"bob\", NULL]",
            vec![
                ScalarValue::Null,
                ScalarValue::String("bob".to_string()),
                ScalarValue::Null,
            ],
        ),
        (
            "[1, 2, 3, 4, 5]",
            vec![
                ScalarValue::Integer(1),
                ScalarValue::Integer(2),
                ScalarValue::Integer(3),
                ScalarValue::Integer(4),
                ScalarValue::Integer(5),
            ],
        ),
        (
            "[42, 3.14, true, \"x\"]",
            vec![
                ScalarValue::Integer(42),
                ScalarValue::Floating(3.14),
                ScalarValue::Boolean(true),
                ScalarValue::String("x".to_string()),
            ],
        ),
    ];

    println!("\n=== Typical row encoding size comparison ===\n");
    println!(
        "{:<25} {:>7}  {:>7}  {:>7}",
        "Row", "current", "tagged", "native"
    );
    println!("{}", "-".repeat(55));
    for (label, row) in &rows {
        let cur = encode_row_current(row);
        let tag = encode_row_tagged(row);
        let nat = encode_row_native(row);
        println!(
            "{:<25} {:>7}  {:>7}  {:>7}",
            label,
            cur.len(),
            tag.len(),
            nat.len()
        );
    }

    println!("\n=== Explanation of current encoding ===");
    println!("serde derive maps enum variants to CBOR maps: {{\"VariantName\": payload}}");
    println!("Unit variant (Null) is a CBOR text string: 'Null'");
    println!("Tuple variant Integer(42):  a1 67 'Integer' 18 2a  = map(1) text(7) uint");
    println!("                            9 bytes overhead + CBOR integer");
    println!("Tuple variant String(s):    a1 66 'String' <text>  = 9 bytes overhead + text");
    println!("Null unit variant:          64 'Null'              = CBOR text 'Null' (5 bytes)");
    println!("\n=== Option A: integer-tagged array [tag, payload] ===");
    println!("Null  -> [0]       = 81 00               (2 bytes)");
    println!("Int   -> [1, n]    = 82 01 <cbor_int>    (3+ bytes)");
    println!("Float -> [2, f]    = 82 02 fb ...        (11 bytes)");
    println!("Bool  -> [3, b]    = 82 03 f4/f5         (3 bytes)");
    println!("Str   -> [4, s]    = 82 04 <cbor_text>   (3+ bytes)");
    println!("Blob  -> [5, b]    = 82 05 <cbor_bytes>  (3+ bytes)");
    println!("\n=== Option B: native CBOR types (no wrapper) ===");
    println!("Null  -> f6                              (1 byte)");
    println!("Int   -> <cbor_int>                      (1-9 bytes)");
    println!("Float -> fb ...                          (9 bytes)");
    println!("Bool  -> f4/f5                           (1 byte)");
    println!("Str   -> <cbor_text>                     (1+ bytes)");
    println!("Blob  -> <cbor_bytes>                    (1+ bytes)");
    println!("\nNote: Option B requires custom Serialize/Deserialize impl.");
    println!("Deserialize maps CBOR type -> ScalarValue variant.");
    println!("No ambiguity: CBOR int=Integer, float=Floating, bool=Boolean,");
    println!("null=Null, text=String, bytes=Blob.");
}
