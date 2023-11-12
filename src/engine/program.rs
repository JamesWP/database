use super::scalarvalue::ScalarValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reg(usize);

#[derive(Clone, Debug)]
pub enum MoveOperation {
    First,
}

// TODO: switch to using {} and named members
#[derive(Clone, Debug)]
pub enum Operation {
    // Value
    StoreValue(Reg, ScalarValue),
    IncrementValue(Reg),
    AddValue(Reg, Reg, Reg),
    MultiplyValue(Reg, Reg, Reg),
    LessThanValue(Reg, Reg, Reg),

    // Db
    Open(Reg, String),
    MoveCursor(Reg, MoveOperation),
    ReadCursor(Vec<Reg>, Reg), // TODO: allow program to select which columns to read and type check

    // Control Flow
    Yield(Vec<Reg>),
    GoTo(usize),
    GoToIfEqualValue(usize, Reg, Reg),
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

        match op {
            Operation::Halt => {}
            _ => self.curent_operation_index += 1,
        };

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
