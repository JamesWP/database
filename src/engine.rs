use crate::storage;

use self::program::{ProgramCode, ScalarValue};

mod program;

#[derive(Clone, Debug)]
enum RegisterValue {
    ScalarValue(ScalarValue)
}

enum StepResult {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
}

#[derive(Clone, Debug)]
struct Registers {
    file: Vec<RegisterValue>
}

struct Engine {
    btree: storage::BTree,
    registers: Registers,
    program: ProgramCode,
}

impl Engine {
    pub fn new(btree: storage::BTree, registers: Registers, program: ProgramCode) -> Engine {
        Engine { btree, registers, program }
    }

    pub fn step(&mut self) -> StepResult {
        todo!()
    }
}