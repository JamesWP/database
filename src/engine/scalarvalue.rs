//TODO: maybe consider removing boolean and making this type only handle numeric types
#[derive(Clone, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
    Boolean(bool),
    String(String),
    Null,
}

impl Eq for ScalarValue {}

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
                    | (_, ScalarValue::String(_)) => {
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
            ScalarValue::Boolean(_) | ScalarValue::String(_) => {
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
            (ScalarValue::Boolean(_), _) | (_, ScalarValue::Boolean(_)) => {
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
            (ScalarValue::Boolean(_), ScalarValue::Boolean(_)) => None,
            (_, _) => None,
        }
    }
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
            _ => false,
        }
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use colored::Colorize;
        match self {
            ScalarValue::Integer(i) => write!(f, "{}", i.to_string().green()),
            ScalarValue::Floating(fl) => write!(f, "{}", fl.to_string().green()),
            ScalarValue::Boolean(b) => write!(f, "{}", b.to_string().green()),
            ScalarValue::String(s) => write!(f, "{}", format!("\"{}\"", s).green()),
            ScalarValue::Null => write!(f, "{}", "NULL".dimmed()),
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
