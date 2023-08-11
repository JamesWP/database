#[derive(Clone, Debug)]
pub struct Reg(usize);

#[derive(Clone, Debug)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
}

#[derive(Clone, Debug)]
pub enum Operation {
    StoreValue(Reg, ScalarValue),
    Yield(Vec<Reg>),
    Halt
}

pub(crate) struct ProgramCode {
    operations: Vec<Operation>,
    curent_operation_index: usize,
}

impl From<&[Operation]> for ProgramCode {
    fn from(value: &[Operation]) -> Self {
        Self { operations: value.to_vec(), curent_operation_index: 0 }
    }
}

impl ProgramCode {
    pub fn advance(&mut self) -> Operation {
        let op = self.curent();
        self.curent_operation_index += 1;

        op
    }

    fn curent(&self) -> Operation {
        self.operations.get(self.curent_operation_index).unwrap().clone()
    }
}

impl Reg {
    pub fn index(&self) -> usize {
        let Reg(index) = self;

        *index
    }
}