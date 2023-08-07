#[derive(Clone, Debug)]
struct Reg(u32);

#[derive(Clone, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
}

#[derive(Clone, Debug)]
enum Operation {
    StoreValue(Reg, ScalarValue)
}

pub(crate) struct ProgramCode {
    operations: Vec<Operation>,
    curent_instruction: u32,
}

impl From<&[Operation]> for ProgramCode {
    fn from(value: &[Operation]) -> Self {
        Self { operations: value.to_vec(), curent_instruction: 0 }
    }
}