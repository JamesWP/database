//TODO: maybe consider removing boolean and making this type only handle numeric types
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
    Boolean(bool),
    String(String),
    Blob(#[serde(with = "serde_bytes")] Vec<u8>),
    Null,
}

impl Eq for ScalarValue {}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ScalarValue::Integer(i) => {
                0u8.hash(state);
                i.hash(state);
            }
            ScalarValue::Floating(f) => {
                1u8.hash(state);
                // Use to_bits() for consistent hashing of floats
                f.to_bits().hash(state);
            }
            ScalarValue::Boolean(b) => {
                2u8.hash(state);
                b.hash(state);
            }
            ScalarValue::String(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            ScalarValue::Blob(b) => {
                4u8.hash(state);
                b.hash(state);
            }
            ScalarValue::Null => {
                5u8.hash(state);
            }
        }
    }
}

macro_rules! numeric_ops {
    ($treight: path, $function: ident) => {
        impl $treight for ScalarValue {
            type Output = ScalarValue;

            fn $function(self, rhs: Self) -> Self::Output {
                use $treight as t;
                let i_op = t::<i64>::$function;
                let f_op = t::<f64>::$function;

                match (self, rhs) {
                    // NULL propagation: any operation with NULL returns NULL
                    (ScalarValue::Null, _) | (_, ScalarValue::Null) => ScalarValue::Null,
                    (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => {
                        ScalarValue::Integer(i_op(lhs, rhs))
                    }
                    (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                        ScalarValue::Floating(f_op(lhs as f64, rhs))
                    }
                    (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                        ScalarValue::Floating(f_op(lhs, rhs as f64))
                    }
                    (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => {
                        ScalarValue::Floating(f_op(lhs, rhs))
                    }
                    (ScalarValue::Boolean(_), _)
                    | (_, ScalarValue::Boolean(_))
                    | (ScalarValue::String(_), _)
                    | (_, ScalarValue::String(_))
                    | (ScalarValue::Blob(_), _)
                    | (_, ScalarValue::Blob(_)) => {
                        panic!("invalid types for numeric operation")
                    }
                }
            }
        }
    };
}

numeric_ops!(core::ops::Mul, mul);
numeric_ops!(core::ops::Sub, sub);
numeric_ops!(core::ops::Div, div);
numeric_ops!(core::ops::Rem, rem);

impl core::ops::Neg for ScalarValue {
    type Output = ScalarValue;

    fn neg(self) -> Self::Output {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::Integer(v) => ScalarValue::Integer(-v),
            ScalarValue::Floating(v) => ScalarValue::Floating(-v),
            ScalarValue::Boolean(_) | ScalarValue::String(_) | ScalarValue::Blob(_) => {
                panic!("cannot negate non-numeric type")
            }
        }
    }
}

impl core::ops::Add for ScalarValue {
    type Output = ScalarValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // NULL propagation: any operation with NULL returns NULL
            (ScalarValue::Null, _) | (_, ScalarValue::Null) => ScalarValue::Null,
            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => {
                ScalarValue::Integer(lhs + rhs)
            }
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs as f64 + rhs)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                ScalarValue::Floating(lhs + rhs as f64)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs + rhs)
            }
            (ScalarValue::String(lhs), ScalarValue::String(rhs)) => ScalarValue::String(lhs + &rhs),
            (ScalarValue::Boolean(_), _)
            | (_, ScalarValue::Boolean(_))
            | (ScalarValue::Blob(_), _)
            | (_, ScalarValue::Blob(_)) => {
                panic!("invalid types for add operation")
            }
            (ScalarValue::String(_), _) | (_, ScalarValue::String(_)) => {
                panic!("cannot add string and non-string types")
            }
        }
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        match (self, rhs) {
            // NULL comparison always returns None (SQL NULL semantics)
            (ScalarValue::Null, _) | (_, ScalarValue::Null) => None,
            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                (*lhs as f64).partial_cmp(rhs)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                lhs.partial_cmp(&(*rhs as f64))
            }
            (ScalarValue::String(lhs), ScalarValue::String(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Blob(lhs), ScalarValue::Blob(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Boolean(_), ScalarValue::Boolean(_)) => None,
            (_, _) => None,
        }
    }
}

/// Ord implementation for sorting
/// NULL is ordered before all other values
/// For floats, NaN is ordered before all other floats
/// Mixed-type comparisons use a type precedence: Null < Boolean < Integer/Floating < String
impl Ord for ScalarValue {
    fn cmp(&self, rhs: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, rhs) {
            (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
            (ScalarValue::Null, _) => Ordering::Less,
            (_, ScalarValue::Null) => Ordering::Greater,

            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => lhs.cmp(rhs),
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => {
                // Handle NaN: NaN < all other floats
                match (lhs.is_nan(), rhs.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => lhs.partial_cmp(rhs).unwrap(),
                }
            }
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                let lhs_f = *lhs as f64;
                if rhs.is_nan() {
                    Ordering::Greater
                } else {
                    lhs_f.partial_cmp(rhs).unwrap()
                }
            }
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                let rhs_f = *rhs as f64;
                if lhs.is_nan() {
                    Ordering::Less
                } else {
                    lhs.partial_cmp(&rhs_f).unwrap()
                }
            }
            (ScalarValue::String(lhs), ScalarValue::String(rhs)) => lhs.cmp(rhs),
            (ScalarValue::Blob(lhs), ScalarValue::Blob(rhs)) => lhs.cmp(rhs),
            (ScalarValue::Boolean(lhs), ScalarValue::Boolean(rhs)) => lhs.cmp(rhs),

            // Mixed-type ordering: Boolean < Integer/Floating < String < Blob
            (ScalarValue::Boolean(_), ScalarValue::Integer(_)) => Ordering::Less,
            (ScalarValue::Boolean(_), ScalarValue::Floating(_)) => Ordering::Less,
            (ScalarValue::Boolean(_), ScalarValue::String(_)) => Ordering::Less,
            (ScalarValue::Boolean(_), ScalarValue::Blob(_)) => Ordering::Less,
            (ScalarValue::Integer(_), ScalarValue::Boolean(_)) => Ordering::Greater,
            (ScalarValue::Floating(_), ScalarValue::Boolean(_)) => Ordering::Greater,
            (ScalarValue::Integer(_), ScalarValue::String(_)) => Ordering::Less,
            (ScalarValue::Integer(_), ScalarValue::Blob(_)) => Ordering::Less,
            (ScalarValue::Floating(_), ScalarValue::String(_)) => Ordering::Less,
            (ScalarValue::Floating(_), ScalarValue::Blob(_)) => Ordering::Less,
            (ScalarValue::String(_), ScalarValue::Integer(_)) => Ordering::Greater,
            (ScalarValue::String(_), ScalarValue::Floating(_)) => Ordering::Greater,
            (ScalarValue::String(_), ScalarValue::Boolean(_)) => Ordering::Greater,
            (ScalarValue::String(_), ScalarValue::Blob(_)) => Ordering::Less,
            (ScalarValue::Blob(_), ScalarValue::Boolean(_)) => Ordering::Greater,
            (ScalarValue::Blob(_), ScalarValue::Integer(_)) => Ordering::Greater,
            (ScalarValue::Blob(_), ScalarValue::Floating(_)) => Ordering::Greater,
            (ScalarValue::Blob(_), ScalarValue::String(_)) => Ordering::Greater,
        }
    }
}

impl ScalarValue {
    /// Extract as string reference, similar to serde_json::Value::as_str()
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Extract as u64, similar to serde_json::Value::as_u64()
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            ScalarValue::Integer(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }

    /// LENGTH(s) returns the length of a string, or NULL for NULL.
    /// For non-string types, converts to string first.
    pub fn length(&self) -> ScalarValue {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::String(s) => ScalarValue::Integer(s.len() as i64),
            ScalarValue::Blob(b) => ScalarValue::Integer(b.len() as i64),
            ScalarValue::Integer(i) => ScalarValue::Integer(i.to_string().len() as i64),
            ScalarValue::Floating(f) => ScalarValue::Integer(f.to_string().len() as i64),
            ScalarValue::Boolean(b) => ScalarValue::Integer(b.to_string().len() as i64),
        }
    }

    /// UPPER(s) returns the uppercase version of a string, or NULL for NULL.
    /// For non-string types, converts to string first.
    pub fn to_uppercase(&self) -> ScalarValue {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::String(s) => ScalarValue::String(s.to_uppercase()),
            ScalarValue::Integer(i) => ScalarValue::String(i.to_string().to_uppercase()),
            ScalarValue::Floating(f) => ScalarValue::String(f.to_string().to_uppercase()),
            ScalarValue::Boolean(b) => ScalarValue::String(b.to_string().to_uppercase()),
            ScalarValue::Blob(_) => panic!("UPPER requires string type"),
        }
    }

    /// LOWER(s) returns the lowercase version of a string, or NULL for NULL.
    /// For non-string types, converts to string first.
    pub fn to_lowercase(&self) -> ScalarValue {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::String(s) => ScalarValue::String(s.to_lowercase()),
            ScalarValue::Integer(i) => ScalarValue::String(i.to_string().to_lowercase()),
            ScalarValue::Floating(f) => ScalarValue::String(f.to_string().to_lowercase()),
            ScalarValue::Boolean(b) => ScalarValue::String(b.to_string().to_lowercase()),
            ScalarValue::Blob(_) => panic!("LOWER requires string type"),
        }
    }

    /// ABS(n) returns the absolute value of a number, or NULL for NULL.
    /// For non-numeric types, panics.
    pub fn abs(&self) -> ScalarValue {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::Integer(i) => ScalarValue::Integer(i.abs()),
            ScalarValue::Floating(f) => ScalarValue::Floating(f.abs()),
            ScalarValue::Boolean(_) | ScalarValue::String(_) | ScalarValue::Blob(_) => {
                panic!("ABS requires numeric type")
            }
        }
    }

    /// SQL LIKE pattern matching
    /// Supports:
    /// - % matches any sequence of characters (including empty)
    /// - _ matches exactly one character
    pub fn like_match(&self, pattern: &ScalarValue) -> bool {
        match (self, pattern) {
            (ScalarValue::String(text), ScalarValue::String(pat)) => sql_like_match(text, pat),
            (ScalarValue::Null, _) | (_, ScalarValue::Null) => false,
            _ => false, // Non-string comparisons return false
        }
    }

    /// Return a plain (no ANSI color codes) string representation for display,
    /// e.g. for computing column widths in tabular output.
    pub fn plain_string(&self) -> String {
        match self {
            ScalarValue::Integer(i) => i.to_string(),
            ScalarValue::Floating(f) => f.to_string(),
            ScalarValue::Boolean(b) => b.to_string(),
            ScalarValue::String(s) => format!("\"{}\"", s),
            ScalarValue::Blob(b) => format!("Blob({})", b.len()),
            ScalarValue::Null => "NULL".to_string(),
        }
    }
}

/// Implement SQL LIKE pattern matching using dynamic programming
fn sql_like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    let text_len = text_chars.len();
    let pat_len = pattern_chars.len();

    // dp[i][j] = true if first i chars of text match first j chars of pattern
    let mut dp = vec![vec![false; pat_len + 1]; text_len + 1];

    // Empty pattern matches empty text
    dp[0][0] = true;

    // Handle patterns starting with % (can match empty string)
    for j in 1..=pat_len {
        if pattern_chars[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    // Fill the DP table
    for i in 1..=text_len {
        for j in 1..=pat_len {
            let text_char = text_chars[i - 1];
            let pat_char = pattern_chars[j - 1];

            if pat_char == '%' {
                // % can match empty or one or more characters
                dp[i][j] = dp[i][j - 1] // match empty
                        || dp[i - 1][j]; // match one or more
            } else if pat_char == '_' || pat_char == text_char {
                // _ matches any single char, or exact match
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[text_len][pat_len]
}

/// Only implemented for testing purposes, actual code shouldn't compare these types directly
#[cfg(test)]
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // NULL is never equal to anything, including itself (SQL semantics)
            (Self::Null, _) | (_, Self::Null) => false,
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Floating(left), Self::Floating(right)) => (left - right).abs() < 0.00001,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Blob(left), Self::Blob(right)) => left == right,
            _ => false,
        }
    }
}

#[cfg(not(test))]
impl PartialEq for ScalarValue {
    fn eq(&self, right: &Self) -> bool {
        match (self, right) {
            // NULL is never equal to anything, including itself (SQL semantics)
            (Self::Null, _) | (_, Self::Null) => false,
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Floating(left), Self::Floating(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Blob(left), Self::Blob(right)) => left == right,
            _ => false,
        }
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Integer(i) => write!(f, "{i}"),
            ScalarValue::Floating(fl) => write!(f, "{fl}"),
            ScalarValue::Boolean(b) => write!(f, "{b}"),
            ScalarValue::String(s) => write!(f, "\"{s}\""),
            ScalarValue::Blob(b) => write!(f, "Blob({})", b.len()),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_equality() {
        let s1 = ScalarValue::String("hello".to_string());
        let s2 = ScalarValue::String("hello".to_string());
        let s3 = ScalarValue::String("world".to_string());

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_string_ordering() {
        let a = ScalarValue::String("apple".to_string());
        let b = ScalarValue::String("banana".to_string());
        let a2 = ScalarValue::String("apple".to_string());

        assert!(a < b);
        assert!(b > a);
        assert!(!(a < a2));
        assert!(!(a > a2));
    }

    #[test]
    fn test_string_concatenation() {
        let s1 = ScalarValue::String("hello".to_string());
        let s2 = ScalarValue::String(" world".to_string());
        let result = s1 + s2;

        assert_eq!(result, ScalarValue::String("hello world".to_string()));
    }

    #[test]
    #[should_panic(expected = "cannot add string and non-string types")]
    fn test_string_add_integer_panics() {
        let s = ScalarValue::String("hello".to_string());
        let i = ScalarValue::Integer(42);
        let _ = s + i;
    }

    #[test]
    #[should_panic(expected = "invalid types for numeric operation")]
    fn test_string_multiply_panics() {
        let s1 = ScalarValue::String("hello".to_string());
        let s2 = ScalarValue::String("world".to_string());
        let _ = s1 * s2;
    }

    #[test]
    fn test_mixed_type_comparison_returns_none() {
        let s = ScalarValue::String("hello".to_string());
        let i = ScalarValue::Integer(42);

        assert!(s.partial_cmp(&i).is_none());
    }

    #[test]
    fn test_null_arithmetic() {
        let null = ScalarValue::Null;
        let one = ScalarValue::Integer(1);

        // NULL + 1 = NULL
        assert!(matches!(null.clone() + one.clone(), ScalarValue::Null));
        // 1 + NULL = NULL
        assert!(matches!(one.clone() + null.clone(), ScalarValue::Null));
        // NULL * 2 = NULL
        assert!(matches!(
            null.clone() * ScalarValue::Integer(2),
            ScalarValue::Null
        ));
        // NULL - NULL = NULL
        assert!(matches!(null.clone() - null.clone(), ScalarValue::Null));
    }

    #[test]
    fn test_null_comparison() {
        let null = ScalarValue::Null;
        let one = ScalarValue::Integer(1);

        // NULL compared to anything returns None (SQL NULL semantics)
        assert_eq!(null.partial_cmp(&null), None);
        assert_eq!(null.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&null), None);
    }

    #[test]
    fn test_null_equality() {
        let null = ScalarValue::Null;
        let one = ScalarValue::Integer(1);

        // In SQL, NULL = NULL is NULL (not true), so PartialEq should return false
        assert_ne!(null, null);
        assert_ne!(null, one);
        assert_ne!(one, null);
    }

    #[test]
    fn test_null_negation() {
        let null = ScalarValue::Null;
        // -NULL = NULL
        assert!(matches!(-null, ScalarValue::Null));
    }
}
