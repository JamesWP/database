use crate::{engine::registers::RegisterValue, storage};

use self::{
    program::{ProgramCode, Reg, ScalarValue},
    registers::Registers,
};

mod program;
mod registers;

type StepResult = std::result::Result<StepSuccess, EngineError>;

#[derive(PartialEq, Debug)]
enum StepSuccess {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
    Error(EngineError),
}

#[derive(PartialEq, Eq, Debug)]
enum EngineError {
    RegisterTypeError(Reg, &'static str, RegisterValue),
}

struct Engine {
    btree: Option<storage::BTree>,
    registers: Registers,
    program: ProgramCode,
}

impl Engine {
    pub fn new(registers: Registers, program: ProgramCode) -> Engine {
        Engine {
            btree: None,
            registers,
            program,
        }
    }

    pub fn step(&mut self) -> StepResult {
        use program::Operation::*;

        match self.program.advance() {
            StoreValue(reg, scalar) => {
                *self.registers.get_mut(reg) = RegisterValue::ScalarValue(scalar);
            }
            Yield(regs) => {
                let values = self.registers.get_range(&regs);
                let values = values
                    .map(RegisterValue::scalar)
                    .map(Option::unwrap)
                    .cloned()
                    .collect();

                return StepResult::Ok(StepSuccess::Yield(values));
            }
            IncrementValue(reg) => {
                if let Some(value) = self.registers.get_mut(reg).integer_mut() {
                    *value += 1;
                } else {
                    return StepResult::Err(EngineError::RegisterTypeError(
                        reg,
                        "expected integer to increment",
                        self.registers.get(reg).clone(),
                    ));
                }
            }
            GoTo(index) => {
                self.program.set_next_operation_index(index);
            }
            GoToIfEqual(index, reg, test_value) => {
                if let Some(reg_value) = self.registers.get(reg).integer() {
                    if test_value == reg_value {
                        self.program.set_next_operation_index(index);
                    } else {
                        // branch not taken
                    }
                } else {
                    return StepResult::Err(EngineError::RegisterTypeError(
                        reg,
                        "expected integer to compare",
                        self.registers.get(reg).clone(),
                    ));
                }
            }
            Halt => {
                return StepResult::Ok(StepSuccess::Halt);
            }
        };

        StepResult::Ok(StepSuccess::Continue)
    }
}

#[cfg(test)]
mod test {
    use crate::engine::{
        program::{Operation, ProgramCode, ScalarValue},
        StepResult, StepSuccess,
    };

    use super::{program::Reg, registers::Registers, Engine};

    #[test]
    fn test_simple_program() {
        let operations = &[
            Operation::StoreValue(Reg::new(0), ScalarValue::Integer(1)),
            Operation::Yield(vec![Reg::new(0)]),
            Operation::Halt,
        ];
        let program: ProgramCode = operations.as_slice().into();
        let registers = Registers::new(1);
        let mut engine = Engine::new(registers, program);

        assert_eq!(engine.step().unwrap(), StepSuccess::Continue);
        assert_eq!(
            engine.step().unwrap(),
            StepSuccess::Yield(vec![ScalarValue::Integer(1)])
        );
        assert_eq!(engine.step().unwrap(), StepSuccess::Halt);
    }

    #[test]
    fn test_increment() {
        let r0 = Reg::new(0);

        let operations = &[
            Operation::StoreValue(r0, ScalarValue::Integer(1)),
            Operation::IncrementValue(r0),
            Operation::Yield(vec![r0]),
            Operation::Halt,
        ];

        let program: ProgramCode = operations.as_slice().into();
        let registers = Registers::new(1);
        let mut engine = Engine::new(registers, program);

        assert_eq!(engine.step().unwrap(), StepSuccess::Continue);
        assert_eq!(engine.step().unwrap(), StepSuccess::Continue);
        assert_eq!(
            engine.step().unwrap(),
            StepSuccess::Yield(vec![ScalarValue::Integer(2)])
        );
        assert_eq!(engine.step().unwrap(), StepSuccess::Halt);
    }

    #[test]
    fn test_goto() {
        let r0 = Reg::new(0);

        let operations = &[
            Operation::StoreValue(r0, ScalarValue::Integer(1)),
            Operation::GoTo(3),
            Operation::IncrementValue(r0),
            Operation::Yield(vec![r0]),
            Operation::Halt,
        ];

        let program: ProgramCode = operations.as_slice().into();
        let registers = Registers::new(1);
        let mut engine = Engine::new(registers, program);

        assert_eq!(engine.step().unwrap(), StepSuccess::Continue);
        assert_eq!(engine.step().unwrap(), StepSuccess::Continue);
        assert_eq!(
            engine.step().unwrap(),
            StepSuccess::Yield(vec![ScalarValue::Integer(1)])
        );
        assert_eq!(engine.step().unwrap(), StepSuccess::Halt);
    }

    #[test]
    fn test_goto_loop() {
        let r0 = Reg::new(0);

        let operations = &[
            Operation::StoreValue(r0, ScalarValue::Integer(1)),
            Operation::IncrementValue(r0),
            Operation::GoToIfEqual(4, r0, 10),
            Operation::GoTo(1),
            Operation::Yield(vec![r0]),
            Operation::Halt,
        ];

        let program: ProgramCode = operations.as_slice().into();
        let registers = Registers::new(1);
        let mut engine = Engine::new(registers, program);

        let mut yielded = false;

        loop {
            match engine.step() {
                StepResult::Ok(StepSuccess::Continue) => {}
                StepResult::Ok(StepSuccess::Yield(values)) => {
                    assert_eq!(values, &[ScalarValue::Integer(10)]);
                    yielded = true;
                }
                StepResult::Ok(StepSuccess::Halt) => {
                    assert!(yielded);
                    break;
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }
}
