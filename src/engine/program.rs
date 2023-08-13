#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reg(usize);

#[derive(Clone, Copy, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
}

#[derive(Clone, Debug)]
pub enum Operation {
    StoreValue(Reg, ScalarValue),
    IncrementValue(Reg),
    AddValue(Reg, Reg, ScalarValue),
    MultiplyValue(Reg, Reg, ScalarValue),
    Yield(Vec<Reg>),
    GoTo(usize),
    GoToIfEqual(usize, Reg, i64),
    Halt,
}

pub(crate) struct ProgramCode {
    operations: Vec<Operation>,
    curent_operation_index: usize,
}

impl From<&[Operation]> for ProgramCode {
    fn from(value: &[Operation]) -> Self {
        Self {
            operations: value.to_vec(),
            curent_operation_index: 0,
        }
    }
}

impl ProgramCode {
    pub fn advance(&mut self) -> Operation {
        let op = self.curent();
        self.curent_operation_index += 1;

        op
    }

    fn curent(&self) -> Operation {
        self.operations
            .get(self.curent_operation_index)
            .unwrap()
            .clone()
    }

    pub(crate) fn set_next_operation_index(&mut self, index: usize) {
        self.curent_operation_index = index;
    }
}

impl Reg {
    pub fn index(&self) -> usize {
        let Reg(index) = self;

        *index
    }

    pub fn new(index: usize) -> Reg {
        Reg(index)
    }
}

impl Eq for ScalarValue {}

impl core::ops::Add for ScalarValue {
    type Output = ScalarValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
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
        }
    }
}

impl core::ops::Mul for ScalarValue {
    type Output = ScalarValue;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (ScalarValue::Integer(lhs), ScalarValue::Integer(rhs)) => {
                ScalarValue::Integer(lhs * rhs)
            }
            (ScalarValue::Integer(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs as f64 * rhs)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Integer(rhs)) => {
                ScalarValue::Floating(lhs * rhs as f64)
            }
            (ScalarValue::Floating(lhs), ScalarValue::Floating(rhs)) => {
                ScalarValue::Floating(lhs * rhs)
            }
        }
    }
}

/// Only implemented for testing purposes, actual code shouldn't compare these types directly
#[cfg(test)]
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Floating(left), Self::Floating(right)) => (left - right).abs() < 0.00001,
            _ => false,
        }
    }
}

#[cfg(not(test))]
impl PartialEq for ScalarValue {
    fn eq(&self, _other: &Self) -> bool {
        panic!("Equality not supported outside of tests");
    }
}
