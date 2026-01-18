//TODO: maybe consider removing boolean and making this type only handle numeric types
#[derive(Clone, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
    Boolean(bool),
    String(String),
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

impl core::ops::Add for ScalarValue {
    type Output = ScalarValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => ScalarValue::Integer(lhs + rhs),
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs as f64 + rhs)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                ScalarValue::Floating(lhs + rhs as f64)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs + rhs)
            }
            (ScalarValue::String(lhs), ScalarValue::String(rhs)) => {
                ScalarValue::String(lhs + &rhs)
            }
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
            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => lhs.partial_cmp(rhs),
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => (*lhs as f64).partial_cmp(rhs),
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => lhs.partial_cmp(&(*rhs as f64)),
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
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Floating(left), Self::Floating(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            _ => false,
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
}
