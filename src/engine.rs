use crate::{storage, engine::registers::RegisterValue};

use self::{
    program::{ProgramCode, ScalarValue},
    registers::Registers,
};

mod program;
mod registers;

enum StepResult {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
}

struct Engine {
    btree: storage::BTree,
    registers: Registers,
    program: ProgramCode,
}

impl Engine {
    pub fn new(btree: storage::BTree, registers: Registers, program: ProgramCode) -> Engine {
        Engine {
            btree,
            registers,
            program,
        }
    }

    pub fn step(&mut self) -> StepResult {
        use program::Operation::*;

        match self.program.advance() {
            StoreValue(reg, scalar) => {
                *self.registers.get_mut(reg) = RegisterValue::ScalarValue(scalar);
            },
            Yield(regs) => {
                let values = self.registers.get_range(&regs);
                let values = values.map(RegisterValue::scalar).map(Option::unwrap).cloned().collect();
                
                return StepResult::Yield(values);
            }
            Halt => {
                return StepResult::Halt;
            }
        };

        StepResult::Continue
    }
}
