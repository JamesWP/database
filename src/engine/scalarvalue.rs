#[derive(Clone, Copy, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
    Boolean(bool),
}

impl Eq for ScalarValue {}

macro_rules! core_ops {
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
                    (ScalarValue::Boolean(_), _) => {
                        panic!()
                    }
                    (_, ScalarValue::Boolean(_)) => {
                        panic!()
                    }
                }
            }
        }
    };
}

core_ops!(core::ops::Add, add);

core_ops!(core::ops::Mul, mul);

/// Only implemented for testing purposes, actual code shouldn't compare these types directly
#[cfg(test)]
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Floating(left), Self::Floating(right)) => (left - right).abs() < 0.00001,
            _ => false,
        }
    }
}

#[cfg(not(test))]
impl PartialEq for ScalarValue {
    fn eq(&self, right: &Self) -> bool {
        match (self, right) {
            (Self::Integer(left), Self::Integer(right)) => left == right,
            _ => panic!(),
        }
    }
}
