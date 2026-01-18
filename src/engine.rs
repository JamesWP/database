use crate::{engine::registers::RegisterValue, storage};

use self::{
    program::{ProgramCode, Reg},
    registers::Registers,
    scalarvalue::ScalarValue,
};

pub mod program;
mod registers;
pub mod scalarvalue;

type StepResult = std::result::Result<StepSuccess, EngineError>;

#[derive(PartialEq, Debug)]
enum StepSuccess {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
}

#[derive(Debug)]
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
            IncrementValue(dest) => {
                let lhs = self.registers.get(dest).scalar().unwrap();
                let rhs = ScalarValue::Integer(1);
                let value = RegisterValue::ScalarValue(lhs.clone() + rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            AddValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap().clone();
                let rhs = self.registers.get(rhs).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(lhs + rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            SubtractValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap().clone();
                let rhs = self.registers.get(rhs).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(lhs - rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            MultiplyValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap().clone();
                let rhs = self.registers.get(rhs).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(lhs * rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            DivideValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap().clone();
                let rhs = self.registers.get(rhs).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(lhs / rhs);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            RemainderValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap().clone();
                let rhs = self.registers.get(rhs).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(lhs % rhs);
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
            LessThanOrEqualValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs <= *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            GreaterThanValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs > *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            GreaterThanOrEqualValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs >= *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            EqualsValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs == *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            NotEqualsValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(*lhs != *rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            AndValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).boolean().unwrap();
                let rhs = self.registers.get(rhs).boolean().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(lhs && rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            OrValue(dest, lhs, rhs) => {
                let lhs = self.registers.get(lhs).boolean().unwrap();
                let rhs = self.registers.get(rhs).boolean().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(lhs || rhs));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            NotValue(dest, src) => {
                let src = self.registers.get(src).boolean().unwrap();
                let value = RegisterValue::ScalarValue(ScalarValue::Boolean(!src));
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            NegateValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(-src);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            CopyValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap().clone();
                let value = RegisterValue::ScalarValue(src);
                let dest = self.registers.get_mut(dest);
                *dest = value;
            }
            GoTo(target) => {
                self.program.set_next_operation_index(target.unwrap_resolved());
            }
            GoToIfEqualValue(target, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                if *lhs == *rhs {
                    self.program.set_next_operation_index(target.unwrap_resolved());
                } else {
                    // branch not taken
                }
            }
            GoToIfFalse(target, reg) => {
                let reg = self.registers.get(reg).boolean().unwrap();
                if !reg {
                    self.program.set_next_operation_index(target.unwrap_resolved());
                } else {
                    // branch not taken
                }
            }
            Halt => {
                return StepResult::Ok(StepSuccess::Halt);
            }
            Open(reg, name) => {
                let btree = self.btree.as_ref().unwrap();
                let cursor = btree.open(&name).unwrap();
                *self.registers.get_mut(reg) = RegisterValue::CursorHandle(cursor);
            }
            MoveCursor(reg, operation) => {
                let cursor = self.registers.get_mut(reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readwrite();
                match operation {
                    program::MoveOperation::First => {
                        cursor.first();
                    }
                    program::MoveOperation::Next => {
                        cursor.next();
                    }
                };
            }
            CanReadCursor(dest, reg) => {
                let cursor = self.registers.get_mut(reg).cursor_mut().unwrap();
                let cursor = cursor.open_readonly();
                let value = cursor.get_entry().is_some();
                // we must drop cursror before we can mutate registers
                drop(cursor);
                let value = ScalarValue::Boolean(value);
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(value);
            }
            ReadCursor(regs, cursor_reg) => {
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let cursor = cursor.open_readwrite();
                let mut value = cursor.get_entry().unwrap();
                let values = value.decode_as_json_array();
                // we must drop cursror before we can mutate registers
                drop(cursor);

                for (reg, value) in regs.iter().zip(values) {
                    match value {
                        serde_json::Value::Number(n) => {
                            if n.is_i64() {
                                let value = ScalarValue::Integer(n.as_i64().unwrap());
                                *self.registers.get_mut(*reg) = RegisterValue::ScalarValue(value);
                            } else if n.is_f64() {
                                let value = ScalarValue::Floating(n.as_f64().unwrap());
                                *self.registers.get_mut(*reg) = RegisterValue::ScalarValue(value);
                            } else {
                                todo!()
                            }
                        }
                        serde_json::Value::Bool(b) => {
                            let value = ScalarValue::Boolean(b);
                            *self.registers.get_mut(*reg) = RegisterValue::ScalarValue(value);
                        }
                        _ => todo!(),
                    }
                }
            }
        };

        StepResult::Ok(StepSuccess::Continue)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        engine::{
            program::{JumpTarget, MoveOperation, Operation, ProgramCode},
            scalarvalue::ScalarValue, StepSuccess,
        },
        storage::BTree,
        test::TestDb,
    };

    use super::{
        program::Reg,
        registers::Registers,
        Engine,
    };

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

        fn new_with_btree(
            operations: &[Operation],
            num_registers: usize,
            btree: BTree,
        ) -> TestHarness {
            let program = operations.into();
            let registers = Registers::new(num_registers);
            let mut engine = Engine::new(registers, program);
            engine.btree = Some(btree);
            TestHarness {
                engine: engine,
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
                Operation::GoTo(JumpTarget::addr(3)),
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
                Operation::GoToIfEqualValue(JumpTarget::addr(5), r0, r1),
                Operation::GoTo(JumpTarget::addr(2)),
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
    fn test_subtract() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);
        let r6 = Reg::new(6);

        let mut harness = TestHarness::new(
            &[
                // Integer subtraction: 100 - 42 = 58
                Operation::StoreValue(r0, ScalarValue::Integer(100)),
                Operation::StoreValue(r1, ScalarValue::Integer(42)),
                Operation::SubtractValue(r2, r0, r1),
                // Float subtraction: 10.5 - 3.5 = 7.0
                Operation::StoreValue(r3, ScalarValue::Floating(10.5)),
                Operation::StoreValue(r4, ScalarValue::Floating(3.5)),
                Operation::SubtractValue(r5, r3, r4),
                // Mixed: integer - float (100 - 0.5 = 99.5)
                Operation::SubtractValue(r6, r0, r4),
                Operation::Yield(vec![r2, r5, r6]),
                Operation::Halt,
            ],
            7,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(58));
        assert_eq!(harness.value(0, 1), ScalarValue::Floating(7.0));
        assert_eq!(harness.value(0, 2), ScalarValue::Floating(96.5));
    }

    #[test]
    fn test_divide() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);

        let mut harness = TestHarness::new(
            &[
                // Integer division: 100 / 3 = 33 (truncated)
                Operation::StoreValue(r0, ScalarValue::Integer(100)),
                Operation::StoreValue(r1, ScalarValue::Integer(3)),
                Operation::DivideValue(r2, r0, r1),
                // Float division: 10.0 / 4.0 = 2.5
                Operation::StoreValue(r3, ScalarValue::Floating(10.0)),
                Operation::StoreValue(r4, ScalarValue::Floating(4.0)),
                Operation::DivideValue(r5, r3, r4),
                Operation::Yield(vec![r2, r5]),
                Operation::Halt,
            ],
            6,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(33));
        assert_eq!(harness.value(0, 1), ScalarValue::Floating(2.5));
    }

    #[test]
    fn test_remainder() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);

        let mut harness = TestHarness::new(
            &[
                // Integer remainder: 100 % 3 = 1
                Operation::StoreValue(r0, ScalarValue::Integer(100)),
                Operation::StoreValue(r1, ScalarValue::Integer(3)),
                Operation::RemainderValue(r2, r0, r1),
                // Float remainder: 10.5 % 3.0 = 1.5
                Operation::StoreValue(r3, ScalarValue::Floating(10.5)),
                Operation::StoreValue(r4, ScalarValue::Floating(3.0)),
                Operation::RemainderValue(r5, r3, r4),
                Operation::Yield(vec![r2, r5]),
                Operation::Halt,
            ],
            6,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(1));
        assert_eq!(harness.value(0, 1), ScalarValue::Floating(1.5));
    }

    #[test]
    fn test_comparison_operations() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);
        let r6 = Reg::new(6);
        let r7 = Reg::new(7);
        let r8 = Reg::new(8);
        let r9 = Reg::new(9);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(5)),
                Operation::StoreValue(r1, ScalarValue::Integer(10)),
                Operation::StoreValue(r2, ScalarValue::Integer(5)),
                // 5 < 10 = true
                Operation::LessThanValue(r3, r0, r1),
                // 5 <= 5 = true
                Operation::LessThanOrEqualValue(r4, r0, r2),
                // 10 > 5 = true
                Operation::GreaterThanValue(r5, r1, r0),
                // 5 >= 5 = true
                Operation::GreaterThanOrEqualValue(r6, r0, r2),
                // 5 == 5 = true
                Operation::EqualsValue(r7, r0, r2),
                // 5 != 10 = true
                Operation::NotEqualsValue(r8, r0, r1),
                // 5 == 10 = false
                Operation::EqualsValue(r9, r0, r1),
                Operation::Yield(vec![r3, r4, r5, r6, r7, r8, r9]),
                Operation::Halt,
            ],
            10,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(true));  // 5 < 10
        assert_eq!(harness.value(0, 1), ScalarValue::Boolean(true));  // 5 <= 5
        assert_eq!(harness.value(0, 2), ScalarValue::Boolean(true));  // 10 > 5
        assert_eq!(harness.value(0, 3), ScalarValue::Boolean(true));  // 5 >= 5
        assert_eq!(harness.value(0, 4), ScalarValue::Boolean(true));  // 5 == 5
        assert_eq!(harness.value(0, 5), ScalarValue::Boolean(true));  // 5 != 10
        assert_eq!(harness.value(0, 6), ScalarValue::Boolean(false)); // 5 == 10
    }

    #[test]
    fn test_logical_operations() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);
        let r6 = Reg::new(6);
        let r7 = Reg::new(7);
        let r8 = Reg::new(8);
        let r9 = Reg::new(9);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Boolean(true)),
                Operation::StoreValue(r1, ScalarValue::Boolean(false)),
                // AND truth table
                Operation::AndValue(r2, r0, r0), // true && true = true
                Operation::AndValue(r3, r0, r1), // true && false = false
                Operation::AndValue(r4, r1, r0), // false && true = false
                Operation::AndValue(r5, r1, r1), // false && false = false
                // OR truth table
                Operation::OrValue(r6, r0, r1),  // true || false = true
                Operation::OrValue(r7, r1, r1),  // false || false = false
                // NOT
                Operation::NotValue(r8, r0),    // !true = false
                Operation::NotValue(r9, r1),    // !false = true
                Operation::Yield(vec![r2, r3, r4, r5, r6, r7, r8, r9]),
                Operation::Halt,
            ],
            10,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        // AND
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(true));  // T && T
        assert_eq!(harness.value(0, 1), ScalarValue::Boolean(false)); // T && F
        assert_eq!(harness.value(0, 2), ScalarValue::Boolean(false)); // F && T
        assert_eq!(harness.value(0, 3), ScalarValue::Boolean(false)); // F && F
        // OR
        assert_eq!(harness.value(0, 4), ScalarValue::Boolean(true));  // T || F
        assert_eq!(harness.value(0, 5), ScalarValue::Boolean(false)); // F || F
        // NOT
        assert_eq!(harness.value(0, 6), ScalarValue::Boolean(false)); // !T
        assert_eq!(harness.value(0, 7), ScalarValue::Boolean(true));  // !F
    }

    #[test]
    fn test_negate_and_copy() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);
        let r5 = Reg::new(5);

        let mut harness = TestHarness::new(
            &[
                // Negate integer: -42
                Operation::StoreValue(r0, ScalarValue::Integer(42)),
                Operation::NegateValue(r1, r0),
                // Negate float: -3.14
                Operation::StoreValue(r2, ScalarValue::Floating(3.14)),
                Operation::NegateValue(r3, r2),
                // Copy integer
                Operation::CopyValue(r4, r0),
                // Copy float
                Operation::CopyValue(r5, r2),
                Operation::Yield(vec![r1, r3, r4, r5]),
                Operation::Halt,
            ],
            6,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(-42));
        assert_eq!(harness.value(0, 1), ScalarValue::Floating(-3.14));
        assert_eq!(harness.value(0, 2), ScalarValue::Integer(42));
        assert_eq!(harness.value(0, 3), ScalarValue::Floating(3.14));
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

    #[test]
    fn test_btree_open() {
        let test = TestDb::default();
        let mut btree = test.btree;
        btree.create_tree("test");

        let mut cursor = btree.open("test").unwrap();
        let mut cursor = cursor.open_readwrite();
        cursor.insert(0, b"[12345,6789]".to_vec());
        cursor.insert(1, b"[12345]".to_vec());
        cursor.insert(2, b"[12345]".to_vec());
        cursor.insert(3, b"[12345]".to_vec());
        drop(cursor);

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);

        let mut harness = TestHarness::new_with_btree(
            &[
                // Open Cursor to "test"
                Operation::Open(r0, "test".to_string()),
                // Move Cursor to first record
                Operation::MoveCursor(r0, MoveOperation::First),
                // Read Record Key
                Operation::ReadCursor(vec![r1, r2], r0),
                // Yield Record Key
                Operation::Yield(vec![r1, r2]),
                Operation::Halt,
            ],
            3,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(12345));
        assert_eq!(harness.value(0, 1), ScalarValue::Integer(6789));
    }

    #[test]
    fn test_read_all_data() {
        let test = TestDb::default();
        let mut btree = test.btree;
        btree.create_tree("test");

        let mut cursor = btree.open("test").unwrap();
        let mut cursor = cursor.open_readwrite();
        cursor.insert(0, b"[12345,6789]".to_vec());
        cursor.insert(1, b"[12345,0]".to_vec());
        cursor.insert(2, b"[12345,0]".to_vec());
        cursor.insert(3, b"[12345,0]".to_vec());
        drop(cursor);

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(r0, "test".to_string()),
                Operation::MoveCursor(r0, MoveOperation::First),
                Operation::CanReadCursor(r1, r0),  // Next
                Operation::GoToIfFalse(JumpTarget::addr(8), r1), // Goto End
                Operation::ReadCursor(vec![r2, r3], r0),
                Operation::Yield(vec![r2, r3]),
                Operation::MoveCursor(r0, MoveOperation::Next),
                Operation::GoTo(JumpTarget::addr(2)), // Goto Next
                Operation::Halt,    // End
            ],
            4,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 4);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(12345));
        assert_eq!(harness.value(0, 1), ScalarValue::Integer(6789));
        assert_eq!(harness.value(1, 0), ScalarValue::Integer(12345));
        assert_eq!(harness.value(2, 0), ScalarValue::Integer(12345));
        assert_eq!(harness.value(3, 0), ScalarValue::Integer(12345));
    }
}
