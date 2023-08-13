use crate::{engine::registers::RegisterValue, storage};

use self::{
    program::{ProgramCode, Reg},
    registers::Registers,
    scalarvalue::ScalarValue,
};

mod program;
mod registers;
mod scalarvalue;

type StepResult = std::result::Result<StepSuccess, EngineError>;

#[derive(PartialEq, Debug)]
enum StepSuccess {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
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
            AddValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(*lhs + *rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            MultiplyValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(*lhs * *rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            LessThanValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs < *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            GoTo(index) => {
                self.program.set_next_operation_index(index);
            }
            GoToIfEqualValue(index, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                if *lhs == *rhs {
                    self.program.set_next_operation_index(index);
                } else {
                    // branch not taken
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
        program::{Operation, ProgramCode},
        scalarvalue::ScalarValue,
        StepResult, StepSuccess,
    };

    use super::{program::Reg, registers::Registers, Engine};

    struct TestHarness {
        engine: Engine,
        yields: Vec<Vec<ScalarValue>>,
    }

    impl TestHarness {
        fn new(operations: &[Operation], num_registers: usize) -> TestHarness {
            let program: ProgramCode = operations.into();
            let registers = Registers::new(num_registers);
            let engine = Engine::new(registers, program);

            TestHarness {
                engine,
                yields: Vec::default(),
            }
        }

        fn run(&mut self) {
            loop {
                match self.engine.step() {
                    Ok(StepSuccess::Continue) => {
                        continue;
                    }
                    Ok(StepSuccess::Halt) => {
                        break;
                    }
                    Ok(StepSuccess::Yield(values)) => {
                        self.yields.push(values);
                    }
                    Err(_) => todo!(),
                };
            }
        }

        fn num_yields(&self) -> usize {
            self.yields.len()
        }

        fn value(&self, yeild_index: usize, column_index: usize) -> ScalarValue {
            self.yields
                .get(yeild_index)
                .unwrap()
                .get(column_index)
                .unwrap()
                .clone()
        }
    }

    #[test]
    fn test_simple_program() {
        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(Reg::new(0), ScalarValue::Integer(1)),
                Operation::Yield(vec![Reg::new(0)]),
                Operation::Halt,
            ],
            1,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(1));
    }

    #[test]
    fn test_increment() {
        let r0 = Reg::new(0);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(1)),
                Operation::IncrementValue(r0),
                Operation::Yield(vec![r0]),
                Operation::Halt,
            ],
            1,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(2));
    }

    #[test]
    fn test_goto() {
        let r0 = Reg::new(0);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(1)),
                Operation::GoTo(3),
                Operation::IncrementValue(r0),
                Operation::Yield(vec![r0]),
                Operation::Halt,
            ],
            1,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(1));
    }

    #[test]
    fn test_goto_loop() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(1)),
                Operation::StoreValue(r1, ScalarValue::Integer(10)),
                Operation::IncrementValue(r0),
                Operation::GoToIfEqualValue(5, r0, r1),
                Operation::GoTo(2),
                Operation::Yield(vec![r0]),
                Operation::Halt,
            ],
            2,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(10));
    }

    #[test]
    fn test_arith() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);

        let a = 999;
        let b = 100;

        let a_expected = a + 1;
        let b_expected = b * 10;

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(a)),
                Operation::StoreValue(r1, ScalarValue::Integer(b)),
                Operation::StoreValue(r4, ScalarValue::Integer(1)),
                Operation::StoreValue(r5, ScalarValue::Integer(10)),
                Operation::AddValue(r2, r0, r4),
                Operation::MultiplyValue(r3, r1, r5),
                Operation::Yield(vec![r2, r3]),
                Operation::Halt,
            ],
            6,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(a_expected));
        assert_eq!(harness.value(0, 1), ScalarValue::Integer(b_expected));
    }

    #[test]
    fn test_compare() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);
        let r6 = Reg::new(6);
        let r7 = Reg::new(7);
        let r8 = Reg::new(8);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(9999)),
                Operation::StoreValue(r5, ScalarValue::Integer(1)),
                Operation::StoreValue(r6, ScalarValue::Integer(9999)),
                Operation::StoreValue(r7, ScalarValue::Integer(10000)),
                Operation::StoreValue(r8, ScalarValue::Integer(-1)),
                Operation::LessThanValue(r1, r0, r5),
                Operation::LessThanValue(r2, r0, r6),
                Operation::LessThanValue(r3, r0, r7),
                Operation::LessThanValue(r4, r0, r8),
                Operation::Yield(vec![r1, r2, r3, r4]),
                Operation::Halt,
            ],
            9,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(false));
        assert_eq!(harness.value(0, 1), ScalarValue::Boolean(false));
        assert_eq!(harness.value(0, 2), ScalarValue::Boolean(true));
        assert_eq!(harness.value(0, 3), ScalarValue::Boolean(false));
    }
}
