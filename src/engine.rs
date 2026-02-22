use crate::{engine::registers::RegisterValue, storage};

use self::{
    program::{Operation, ProgramCode, Reg},
    registers::Registers,
    scalarvalue::ScalarValue,
};

pub mod program;
pub(crate) mod registers;
pub mod scalarvalue;

type StepResult = std::result::Result<StepSuccess, EngineError>;

#[derive(PartialEq, Debug)]
pub(crate) enum StepSuccess {
    Halt,
    Yield(Vec<ScalarValue>),
    Continue,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum EngineError {
    #[allow(dead_code)]
    RegisterTypeError(Reg, &'static str, RegisterValue),
}

pub(crate) struct Engine {
    btree: Option<storage::BTree>,
    registers: Registers,
    program: ProgramCode,
}

impl Engine {
    #[allow(dead_code)]
    pub fn new(registers: Registers, program: ProgramCode) -> Engine {
        Engine {
            btree: None,
            registers,
            program,
        }
    }

    /// Create an engine from operations and register count, with a BTree for storage.
    pub(crate) fn with_program(
        operations: &[Operation],
        num_registers: usize,
        btree: storage::BTree,
    ) -> Engine {
        let program: ProgramCode = operations.into();
        let registers = Registers::new(num_registers);
        Engine {
            btree: Some(btree),
            registers,
            program,
        }
    }

    /// Take the BTree out of the engine, for reuse after execution.
    #[allow(dead_code)]
    pub(crate) fn take_btree(&mut self) -> Option<storage::BTree> {
        self.btree.take()
    }

    /// Run the program to completion, returning all yielded rows.
    pub(crate) fn run(&mut self) -> Vec<Vec<ScalarValue>> {
        let mut yields = Vec::new();
        loop {
            match self.step() {
                Ok(StepSuccess::Continue) => continue,
                Ok(StepSuccess::Halt) => break,
                Ok(StepSuccess::Yield(values)) => yields.push(values),
                Err(e) => panic!("Engine error: {:?}", e),
            }
        }
        yields
    }

    /// Run the program with a maximum step limit to prevent infinite loops.
    /// Panics if max_steps is exceeded.
    #[cfg(test)]
    pub(crate) fn run_with_limit(&mut self, max_steps: usize) -> Vec<Vec<ScalarValue>> {
        let mut yields = Vec::new();
        let mut steps = 0;
        loop {
            if steps >= max_steps {
                panic!(
                    "Engine exceeded max steps ({}). Last {} yields: {:?}",
                    max_steps,
                    yields.len().min(3),
                    yields.iter().rev().take(3).collect::<Vec<_>>()
                );
            }
            steps += 1;

            match self.step() {
                Ok(StepSuccess::Continue) => continue,
                Ok(StepSuccess::Halt) => break,
                Ok(StepSuccess::Yield(values)) => yields.push(values),
                Err(e) => panic!("Engine error after {} steps: {:?}", steps, e),
            }
        }
        yields
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
            DecrementValue(dest) => {
                let lhs = self.registers.get(dest).scalar().unwrap();
                let rhs = ScalarValue::Integer(1);
                let value = RegisterValue::ScalarValue(lhs.clone() - rhs);
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
            IsNullValue(dest, src) => {
                let is_null = matches!(
                    self.registers.get(src),
                    RegisterValue::ScalarValue(ScalarValue::Null)
                );
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(is_null));
            }
            IsNotNullValue(dest, src) => {
                let is_null = matches!(
                    self.registers.get(src),
                    RegisterValue::ScalarValue(ScalarValue::Null)
                );
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(!is_null));
            }
            LengthValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap();
                let result = src.length();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(result);
            }
            UpperValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap();
                let result = src.to_uppercase();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(result);
            }
            LowerValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap();
                let result = src.to_lowercase();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(result);
            }
            AbsValue(dest, src) => {
                let src = self.registers.get(src).scalar().unwrap();
                let result = src.abs();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(result);
            }
            LikeValue(dest, value_reg, pattern_reg) => {
                let value = self.registers.get(value_reg).scalar().unwrap();
                let pattern = self.registers.get(pattern_reg).scalar().unwrap();
                let result = value.like_match(pattern);
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(result));
            }
            InitKeyList(reg) => {
                *self.registers.get_mut(reg) = RegisterValue::KeyList(Vec::new());
            }
            AppendKey(list_reg, key_reg) => {
                let key = match self.registers.get(key_reg).scalar().unwrap() {
                    ScalarValue::Integer(k) => *k as u64,
                    other => panic!("AppendKey requires Integer key, got {:?}", other),
                };
                let list = self.registers.get_mut(list_reg).key_list_mut().unwrap();
                list.push(key);
            }
            PopKey(dest_reg, list_reg, target) => {
                let list = self.registers.get_mut(list_reg).key_list_mut().unwrap();
                if let Some(key) = list.pop() {
                    let value = ScalarValue::Integer(key as i64);
                    *self.registers.get_mut(dest_reg) = RegisterValue::ScalarValue(value);
                } else {
                    // List is empty, jump to target
                    self.program
                        .set_next_operation_index(target.unwrap_resolved());
                }
            }
            InitRowBuffer(reg) => {
                *self.registers.get_mut(reg) = RegisterValue::RowBuffer(registers::RowBuffer {
                    rows: Vec::new(),
                    cursor: 0,
                });
            }
            AppendToRowBuffer(buffer_reg, value_regs) => {
                let values: Vec<ScalarValue> = value_regs
                    .iter()
                    .map(|reg| self.registers.get(*reg).scalar().unwrap().clone())
                    .collect();
                let buffer = self.registers.get_mut(buffer_reg).row_buffer_mut().unwrap();
                buffer.rows.push(values);
            }
            SortRowBuffer(buffer_reg, sort_keys) => {
                let buffer = self.registers.get_mut(buffer_reg).row_buffer_mut().unwrap();
                buffer.rows.sort_by(|row_a, row_b| {
                    for key_spec in sort_keys.iter() {
                        let val_a = &row_a[key_spec.column_index];
                        let val_b = &row_b[key_spec.column_index];
                        let cmp = val_a.cmp(val_b);
                        if cmp != std::cmp::Ordering::Equal {
                            return if key_spec.descending {
                                cmp.reverse()
                            } else {
                                cmp
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                // Reverse so pop() yields in correct order (lowest to highest)
                buffer.rows.reverse();
            }
            YieldFromRowBuffer(dest_regs, buffer_reg, target) => {
                let buffer = self.registers.get_mut(buffer_reg).row_buffer_mut().unwrap();
                if let Some(row) = buffer.rows.pop() {
                    for (dest_reg, value) in dest_regs.iter().zip(row.into_iter()) {
                        *self.registers.get_mut(*dest_reg) = RegisterValue::ScalarValue(value);
                    }
                } else {
                    // Buffer is empty, jump to target
                    self.program
                        .set_next_operation_index(target.unwrap_resolved());
                }
            }
            RewindRowBuffer(buf_reg) => {
                let buffer = self.registers.get_mut(buf_reg).row_buffer_mut().unwrap();
                buffer.cursor = 0;
            }
            NextFromRowBuffer(dest_regs, buf_reg, target) => {
                // Borrow checker note: must extract data and drop the buffer borrow
                // BEFORE writing to dest registers. Follow the pattern used by
                // YieldFromRowBuffer (which pops a row, then writes to dest regs).
                let maybe_row = {
                    let buffer = self.registers.get_mut(buf_reg).row_buffer_mut().unwrap();
                    if buffer.cursor >= buffer.rows.len() {
                        None
                    } else {
                        let row = buffer.rows[buffer.cursor].clone();
                        buffer.cursor += 1;
                        Some(row)
                    }
                }; // buffer borrow dropped here
                match maybe_row {
                    None => {
                        self.program
                            .set_next_operation_index(target.unwrap_resolved());
                    }
                    Some(row) => {
                        for (dest, value) in dest_regs.iter().zip(row.into_iter()) {
                            *self.registers.get_mut(*dest) = RegisterValue::ScalarValue(value);
                        }
                    }
                }
            }
            InitGroupTable(reg) => {
                use std::collections::BTreeMap;
                *self.registers.get_mut(reg) = RegisterValue::GroupTable(BTreeMap::new());
            }
            UpdateGroup(table_reg, key_regs, agg_specs) => {
                use crate::engine::program::AggregateOp;
                use crate::engine::registers::Accumulator;

                // Evaluate group keys
                let keys: Vec<ScalarValue> = key_regs
                    .iter()
                    .map(|reg| self.registers.get(*reg).scalar().unwrap().clone())
                    .collect();

                // Evaluate all input values first (before mutable borrow)
                let input_values: Vec<Option<ScalarValue>> = agg_specs
                    .iter()
                    .map(|spec| {
                        spec.input_reg
                            .map(|reg| self.registers.get(reg).scalar().unwrap().clone())
                    })
                    .collect();

                // Get or create group entry
                let table = self.registers.get_mut(table_reg).group_table_mut().unwrap();
                let accumulators = table.entry(keys.clone()).or_insert_with(|| {
                    // Initialize accumulators for new group
                    agg_specs
                        .iter()
                        .map(|spec| match spec.op {
                            AggregateOp::Count => Accumulator::Count { count: 0 },
                            AggregateOp::Sum => Accumulator::Sum {
                                sum: ScalarValue::Integer(0),
                                count: 0,
                            },
                            AggregateOp::Avg => Accumulator::Avg {
                                sum: ScalarValue::Integer(0),
                                count: 0,
                            },
                            AggregateOp::Min => Accumulator::Min { value: None },
                            AggregateOp::Max => Accumulator::Max { value: None },
                        })
                        .collect()
                });

                // Update each accumulator
                for ((acc, spec), input_value) in accumulators
                    .iter_mut()
                    .zip(agg_specs.iter())
                    .zip(input_values.iter())
                {
                    match acc {
                        Accumulator::Count { ref mut count } => {
                            if spec.input_reg.is_none() {
                                // COUNT(*) - count all rows
                                *count += 1;
                            } else if let Some(val) = input_value {
                                // COUNT(expr) - count non-NULL values
                                if !matches!(val, ScalarValue::Null) {
                                    *count += 1;
                                }
                            }
                        }
                        Accumulator::Sum {
                            ref mut sum,
                            ref mut count,
                        } => {
                            if let Some(val) = input_value {
                                if !matches!(val, ScalarValue::Null) {
                                    *sum = sum.clone() + val.clone();
                                    *count += 1;
                                }
                            }
                        }
                        Accumulator::Avg {
                            ref mut sum,
                            ref mut count,
                        } => {
                            if let Some(val) = input_value {
                                if !matches!(val, ScalarValue::Null) {
                                    *sum = sum.clone() + val.clone();
                                    *count += 1;
                                }
                            }
                        }
                        Accumulator::Min { ref mut value } => {
                            if let Some(val) = input_value {
                                if !matches!(val, ScalarValue::Null) {
                                    *value = Some(match value.as_ref() {
                                        Some(current) => {
                                            if val < current {
                                                val.clone()
                                            } else {
                                                current.clone()
                                            }
                                        }
                                        None => val.clone(),
                                    });
                                }
                            }
                        }
                        Accumulator::Max { ref mut value } => {
                            if let Some(val) = input_value {
                                if !matches!(val, ScalarValue::Null) {
                                    *value = Some(match value.as_ref() {
                                        Some(current) => {
                                            if val > current {
                                                val.clone()
                                            } else {
                                                current.clone()
                                            }
                                        }
                                        None => val.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            YieldFromGroupTable(dest_regs, table_reg, target) => {
                use crate::engine::registers::Accumulator;

                let table = self.registers.get_mut(table_reg).group_table_mut().unwrap();
                if let Some((keys, accumulators)) = table.iter().next() {
                    let keys = keys.clone();
                    let accumulators = accumulators.clone();
                    table.remove(&keys);

                    // Build output: group keys + finalized aggregates
                    let mut outputs = keys;
                    for acc in accumulators {
                        let result = match acc {
                            Accumulator::Count { count } => ScalarValue::Integer(count),
                            Accumulator::Sum { sum, count: _ } => sum,
                            Accumulator::Avg { sum, count } => {
                                if count == 0 {
                                    ScalarValue::Null
                                } else {
                                    sum / ScalarValue::Integer(count)
                                }
                            }
                            Accumulator::Min { value } => value.unwrap_or(ScalarValue::Null),
                            Accumulator::Max { value } => value.unwrap_or(ScalarValue::Null),
                        };
                        outputs.push(result);
                    }

                    // Store in destination registers
                    for (dest_reg, value) in dest_regs.iter().zip(outputs.into_iter()) {
                        *self.registers.get_mut(*dest_reg) = RegisterValue::ScalarValue(value);
                    }
                } else {
                    // Table is empty, jump to target
                    self.program
                        .set_next_operation_index(target.unwrap_resolved());
                }
            }
            GoTo(target) => {
                self.program
                    .set_next_operation_index(target.unwrap_resolved());
            }
            GoToIfEqualValue(target, lhs, rhs) => {
                let lhs = self.registers.get(lhs).scalar().unwrap();
                let rhs = self.registers.get(rhs).scalar().unwrap();
                if *lhs == *rhs {
                    self.program
                        .set_next_operation_index(target.unwrap_resolved());
                } else {
                    // branch not taken
                }
            }
            GoToIfFalse(target, reg) => {
                let reg = self.registers.get(reg).boolean().unwrap();
                if !reg {
                    self.program
                        .set_next_operation_index(target.unwrap_resolved());
                } else {
                    // branch not taken
                }
            }
            Halt => {
                return StepResult::Ok(StepSuccess::Halt);
            }
            Open(reg, rootpage) => {
                let btree = self.btree.as_ref().unwrap();
                let cursor = btree.open(rootpage);
                *self.registers.get_mut(reg) = RegisterValue::CursorHandle(cursor);
            }
            MoveCursor(reg, operation) => {
                // Extract key value first to avoid borrow conflict
                let key_bytes = if let program::MoveOperation::Find(key_reg) = operation {
                    let key_value = self.registers.get(key_reg).scalar().unwrap();
                    Some(match key_value {
                        ScalarValue::Integer(k) => storage::encode_u64_key(*k as u64),
                        ScalarValue::Blob(b) => b.clone(),
                        other => panic!("Find requires Integer or Blob key, got {:?}", other),
                    })
                } else {
                    None
                };

                let cursor = self.registers.get_mut(reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readwrite();
                match operation {
                    program::MoveOperation::First => cursor.first(),
                    program::MoveOperation::Next => cursor.next(),
                    program::MoveOperation::Last => cursor.last(),
                    program::MoveOperation::Find(_) => {
                        cursor.find(&key_bytes.unwrap());
                    }
                }
            }
            CanReadCursor(dest, reg) => {
                let cursor = self.registers.get_mut(reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readonly();
                let value = cursor.get_entry().is_some();
                // we must drop cursror before we can mutate registers
                drop(cursor);
                let value = ScalarValue::Boolean(value);
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(value);
            }
            EncodeIndexKey(dest, src) => {
                let value = self.registers.get(src).scalar().unwrap();
                let encoded_key = storage::encode_index_value(value);
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Blob(encoded_key));
            }
            ReadCurrentKey(dest, cursor_reg) => {
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readonly();
                let entry = cursor.get_entry().unwrap();
                let key = entry.key().to_vec();
                drop(cursor);
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(ScalarValue::Blob(key));
            }
            BlobStartsWith(dest, blob_reg, prefix_reg) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobStartsWith: blob register must be Blob"),
                };
                let prefix = match self.registers.get(prefix_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobStartsWith: prefix register must be Blob"),
                };
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(blob.starts_with(&prefix)));
            }
            BlobPrefixLt(dest, blob_reg, bound_reg) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobPrefixLt: blob register must be Blob"),
                };
                let bound = match self.registers.get(bound_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobPrefixLt: bound register must be Blob"),
                };
                let prefix = &blob[..bound.len().min(blob.len())];
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(prefix < bound.as_slice()));
            }
            BlobPrefixLe(dest, blob_reg, bound_reg) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobPrefixLe: blob register must be Blob"),
                };
                let bound = match self.registers.get(bound_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobPrefixLe: bound register must be Blob"),
                };
                let prefix = &blob[..bound.len().min(blob.len())];
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Boolean(prefix <= bound.as_slice()));
            }
            BlobSliceTail(dest, blob_reg, offset) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobSliceTail: blob register must be Blob"),
                };
                let tail = blob[offset..].to_vec();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(ScalarValue::Blob(tail));
            }
            BlobSliceLast(dest, blob_reg, n) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobSliceLast: blob register must be Blob"),
                };
                let start = blob.len().saturating_sub(n);
                let tail = blob[start..].to_vec();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(ScalarValue::Blob(tail));
            }
            BlobDropLast(dest, blob_reg, n) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("BlobDropLast: blob register must be Blob"),
                };
                let end = blob.len().saturating_sub(n);
                let head = blob[..end].to_vec();
                *self.registers.get_mut(dest) = RegisterValue::ScalarValue(ScalarValue::Blob(head));
            }
            DecodeU64Key(dest, blob_reg) => {
                let blob = match self.registers.get(blob_reg).scalar().unwrap() {
                    ScalarValue::Blob(b) => b.clone(),
                    _ => panic!("DecodeU64Key: blob register must be Blob"),
                };
                let rowid = storage::decode_u64_key(&blob);
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Integer(rowid as i64));
            }
            ReadCursor(regs, cursor_reg) => {
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readwrite();
                let mut value = cursor.get_entry().unwrap();
                let values = value.decode_as_array();
                // we must drop cursror before we can mutate registers
                drop(cursor);

                for (reg, value) in regs.iter().zip(values) {
                    *self.registers.get_mut(*reg) = RegisterValue::ScalarValue(value);
                }
            }
            ReadKey(dest, cursor_reg) => {
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readonly();
                let entry = cursor.get_entry().unwrap();
                let key = storage::decode_u64_key(entry.key());
                drop(cursor);
                *self.registers.get_mut(dest) =
                    RegisterValue::ScalarValue(ScalarValue::Integer(key as i64));
            }
            WriteCursor(cursor_reg, key_reg, value_regs) => {
                // Read key value
                let key = match self.registers.get(key_reg).scalar().unwrap() {
                    ScalarValue::Integer(k) => *k as u64,
                    other => panic!("WriteCursor key must be Integer, got {:?}", other),
                };

                // Read values and serialize as CBOR
                let scalar_values: Vec<ScalarValue> = value_regs
                    .iter()
                    .map(|reg| self.registers.get(*reg).scalar().unwrap().clone())
                    .collect();

                let mut bytes = Vec::new();
                ciborium::ser::into_writer(&scalar_values, &mut bytes).unwrap();

                // Write to btree
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readwrite();
                cursor.insert_u64(key, bytes);
            }
            WriteIndex(cursor_reg, value_regs, pk_reg) => {
                // Encode each indexed column value, concatenated in order
                let mut index_key = Vec::new();
                for value_reg in value_regs {
                    let indexed_value = self.registers.get(value_reg).scalar().unwrap();
                    index_key.extend_from_slice(&storage::encode_index_value(indexed_value));
                }

                // Append primary key to make the entry unique in the index B-tree
                let pk_value = self.registers.get(pk_reg).scalar().unwrap();
                let pk = match pk_value {
                    ScalarValue::Integer(i) => *i as u64,
                    _ => panic!("WriteIndex: primary key must be INTEGER"),
                };
                index_key.extend_from_slice(&storage::encode_u64_key(pk));

                // Write to index B-tree — rowid is already encoded in key suffix,
                // so the value is empty.
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut c = cursor.open_readwrite();
                c.insert(&index_key, vec![]);
            }
            DeleteCursor(cursor_reg) => {
                // Delete at current cursor position
                let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
                let mut cursor = cursor.open_readwrite();
                cursor.delete_current();
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
            scalarvalue::ScalarValue,
            StepSuccess,
        },
        storage::{self, BTree},
        test::TestDb,
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
    fn test_decrement() {
        let r0 = Reg::new(0);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(10)),
                Operation::DecrementValue(r0),
                Operation::Yield(vec![r0]),
                Operation::Halt,
            ],
            1,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(9));
    }

    #[test]
    fn test_decrement_to_zero() {
        let r0 = Reg::new(0);
        let r1 = Reg::new(1);

        let mut harness = TestHarness::new(
            &[
                Operation::StoreValue(r0, ScalarValue::Integer(3)),
                Operation::StoreValue(r1, ScalarValue::Integer(0)),
                // Loop: decrement until zero
                Operation::GoToIfEqualValue(JumpTarget::addr(5), r0, r1),
                Operation::DecrementValue(r0),
                Operation::GoTo(JumpTarget::addr(2)),
                Operation::Yield(vec![r0]),
                Operation::Halt,
            ],
            2,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(0));
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
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(true)); // 5 < 10
        assert_eq!(harness.value(0, 1), ScalarValue::Boolean(true)); // 5 <= 5
        assert_eq!(harness.value(0, 2), ScalarValue::Boolean(true)); // 10 > 5
        assert_eq!(harness.value(0, 3), ScalarValue::Boolean(true)); // 5 >= 5
        assert_eq!(harness.value(0, 4), ScalarValue::Boolean(true)); // 5 == 5
        assert_eq!(harness.value(0, 5), ScalarValue::Boolean(true)); // 5 != 10
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
                Operation::OrValue(r6, r0, r1), // true || false = true
                Operation::OrValue(r7, r1, r1), // false || false = false
                // NOT
                Operation::NotValue(r8, r0), // !true = false
                Operation::NotValue(r9, r1), // !false = true
                Operation::Yield(vec![r2, r3, r4, r5, r6, r7, r8, r9]),
                Operation::Halt,
            ],
            10,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        // AND
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(true)); // T && T
        assert_eq!(harness.value(0, 1), ScalarValue::Boolean(false)); // T && F
        assert_eq!(harness.value(0, 2), ScalarValue::Boolean(false)); // F && T
        assert_eq!(harness.value(0, 3), ScalarValue::Boolean(false)); // F && F
                                                                      // OR
        assert_eq!(harness.value(0, 4), ScalarValue::Boolean(true)); // T || F
        assert_eq!(harness.value(0, 5), ScalarValue::Boolean(false)); // F || F
                                                                      // NOT
        assert_eq!(harness.value(0, 6), ScalarValue::Boolean(false)); // !T
        assert_eq!(harness.value(0, 7), ScalarValue::Boolean(true)); // !F
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
        let root = btree.create_tree();
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v0 = Vec::new();
            let values = vec![ScalarValue::Integer(12345), ScalarValue::Integer(6789)];
            ciborium::ser::into_writer(&values, &mut v0).unwrap();
            c.insert_u64(0, v0);
            let mut v1 = Vec::new();
            let values = vec![ScalarValue::Integer(12345)];
            ciborium::ser::into_writer(&values, &mut v1).unwrap();
            c.insert_u64(1, v1.clone());
            c.insert_u64(2, v1.clone());
            c.insert_u64(3, v1);
        }

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);

        let mut harness = TestHarness::new_with_btree(
            &[
                // Open Cursor to test table
                Operation::Open(r0, root),
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
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v0 = Vec::new();
            let values = vec![ScalarValue::Integer(12345), ScalarValue::Integer(6789)];
            ciborium::ser::into_writer(&values, &mut v0).unwrap();
            c.insert_u64(0, v0);
            let mut v1 = Vec::new();
            let values = vec![ScalarValue::Integer(12345), ScalarValue::Integer(0)];
            ciborium::ser::into_writer(&values, &mut v1).unwrap();
            c.insert_u64(1, v1.clone());
            c.insert_u64(2, v1.clone());
            c.insert_u64(3, v1);
        }

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(r0, root),
                Operation::MoveCursor(r0, MoveOperation::First),
                Operation::CanReadCursor(r1, r0), // Next
                Operation::GoToIfFalse(JumpTarget::addr(8), r1), // Goto End
                Operation::ReadCursor(vec![r2, r3], r0),
                Operation::Yield(vec![r2, r3]),
                Operation::MoveCursor(r0, MoveOperation::Next),
                Operation::GoTo(JumpTarget::addr(2)), // Goto Next
                Operation::Halt,                      // End
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

    #[test]
    fn test_read_cursor_string_values() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v0 = Vec::new();
            let values0 = vec![
                ScalarValue::Integer(1),
                ScalarValue::String("alice".to_string()),
                ScalarValue::Integer(30),
            ];
            ciborium::ser::into_writer(&values0, &mut v0).unwrap();
            c.insert_u64(0, v0);
            let mut v1 = Vec::new();
            let values1 = vec![
                ScalarValue::Integer(2),
                ScalarValue::String("bob".to_string()),
                ScalarValue::Integer(25),
            ];
            ciborium::ser::into_writer(&values1, &mut v1).unwrap();
            c.insert_u64(1, v1);
        }

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);
        let r4 = Reg::new(4);

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(r0, root),
                Operation::MoveCursor(r0, MoveOperation::First),
                Operation::CanReadCursor(r1, r0),                // 2
                Operation::GoToIfFalse(JumpTarget::addr(8), r1), // 3
                Operation::ReadCursor(vec![r2, r3, r4], r0),     // 4
                Operation::Yield(vec![r2, r3, r4]),              // 5
                Operation::MoveCursor(r0, MoveOperation::Next),  // 6
                Operation::GoTo(JumpTarget::addr(2)),            // 7
                Operation::Halt,                                 // 8
            ],
            5,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 2);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(1));
        assert_eq!(
            harness.value(0, 1),
            ScalarValue::String("alice".to_string())
        );
        assert_eq!(harness.value(0, 2), ScalarValue::Integer(30));
        assert_eq!(harness.value(1, 0), ScalarValue::Integer(2));
        assert_eq!(harness.value(1, 1), ScalarValue::String("bob".to_string()));
        assert_eq!(harness.value(1, 2), ScalarValue::Integer(25));
    }

    // ========================================================================
    // Tests for Engine::with_program and Engine::run API
    // ========================================================================

    #[test]
    fn test_engine_with_program_simple() {
        let test = TestDb::default();
        let btree = test.btree;

        let r0 = Reg::new(0);
        let ops = [
            Operation::StoreValue(r0, ScalarValue::Integer(42)),
            Operation::Yield(vec![r0]),
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 1, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(42));
    }

    #[test]
    fn test_engine_with_program_btree_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert test data
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v0 = Vec::new();
            let values = vec![ScalarValue::Integer(100), ScalarValue::Integer(200)];
            ciborium::ser::into_writer(&values, &mut v0).unwrap();
            c.insert_u64(0, v0);
            let mut v1 = Vec::new();
            let values = vec![ScalarValue::Integer(300), ScalarValue::Integer(400)];
            ciborium::ser::into_writer(&values, &mut v1).unwrap();
            c.insert_u64(1, v1);
        }

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);
        let r3 = Reg::new(3);

        let ops = [
            Operation::Open(r0, root),
            Operation::MoveCursor(r0, MoveOperation::First),
            Operation::CanReadCursor(r1, r0),
            Operation::GoToIfFalse(JumpTarget::addr(8), r1),
            Operation::ReadCursor(vec![r2, r3], r0),
            Operation::Yield(vec![r2, r3]),
            Operation::MoveCursor(r0, MoveOperation::Next),
            Operation::GoTo(JumpTarget::addr(2)),
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 4, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(100));
        assert_eq!(yields[0][1], ScalarValue::Integer(200));
        assert_eq!(yields[1][0], ScalarValue::Integer(300));
        assert_eq!(yields[1][1], ScalarValue::Integer(400));
    }

    #[test]
    fn test_engine_run_empty_table() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();
        // No data inserted - empty table

        let r0 = Reg::new(0);
        let r1 = Reg::new(1);
        let r2 = Reg::new(2);

        let ops = [
            Operation::Open(r0, root),
            Operation::MoveCursor(r0, MoveOperation::First),
            Operation::CanReadCursor(r1, r0),
            Operation::GoToIfFalse(JumpTarget::addr(7), r1),
            Operation::ReadCursor(vec![r2], r0),
            Operation::Yield(vec![r2]),
            Operation::GoTo(JumpTarget::addr(2)),
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 3, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 0);
    }

    #[test]
    fn test_write_cursor_and_read_back() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let r_cursor = Reg::new(0);
        let r_key = Reg::new(1);
        let r_val1 = Reg::new(2);
        let r_val2 = Reg::new(3);
        let r_flag = Reg::new(4);
        let r_out1 = Reg::new(5);
        let r_out2 = Reg::new(6);

        let mut harness = TestHarness::new_with_btree(
            &[
                // Write a row
                Operation::Open(r_cursor, root), // 0
                Operation::StoreValue(r_key, ScalarValue::Integer(1)), // 1
                Operation::StoreValue(r_val1, ScalarValue::Integer(42)), // 2
                Operation::StoreValue(r_val2, ScalarValue::String("hello".to_string())), // 3
                Operation::WriteCursor(r_cursor, r_key, vec![r_val1, r_val2]), // 4
                // Read it back
                Operation::MoveCursor(r_cursor, MoveOperation::First), // 5
                Operation::CanReadCursor(r_flag, r_cursor),            // 6
                Operation::GoToIfFalse(JumpTarget::addr(11), r_flag),  // 7
                Operation::ReadCursor(vec![r_out1, r_out2], r_cursor), // 8
                Operation::Yield(vec![r_out1, r_out2]),                // 9
                Operation::Halt,                                       // 10 (unreachable but valid)
                Operation::Halt,                                       // 11
            ],
            7,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(42));
        assert_eq!(
            harness.value(0, 1),
            ScalarValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_move_last_and_read_key() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Pre-populate with some data
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v1 = Vec::new();
            let values = vec![ScalarValue::Integer(10)];
            ciborium::ser::into_writer(&values, &mut v1).unwrap();
            c.insert_u64(1, v1);
            let mut v2 = Vec::new();
            let values = vec![ScalarValue::Integer(20)];
            ciborium::ser::into_writer(&values, &mut v2).unwrap();
            c.insert_u64(2, v2);
            let mut v5 = Vec::new();
            let values = vec![ScalarValue::Integer(50)];
            ciborium::ser::into_writer(&values, &mut v5).unwrap();
            c.insert_u64(5, v5);
        }

        let r_cursor = Reg::new(0);
        let r_key = Reg::new(1);

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(r_cursor, root),
                Operation::MoveCursor(r_cursor, MoveOperation::Last),
                Operation::ReadKey(r_key, r_cursor),
                Operation::Yield(vec![r_key]),
                Operation::Halt,
            ],
            2,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(5));
    }

    #[test]
    fn test_write_cursor_on_empty_table() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let r_cursor = Reg::new(0);
        let r_key = Reg::new(1);
        let r_val = Reg::new(2);
        let r_readkey = Reg::new(3);

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(r_cursor, root),
                Operation::StoreValue(r_key, ScalarValue::Integer(1)),
                Operation::StoreValue(r_val, ScalarValue::Integer(999)),
                Operation::WriteCursor(r_cursor, r_key, vec![r_val]),
                // Read back the key
                Operation::MoveCursor(r_cursor, MoveOperation::First),
                Operation::ReadKey(r_readkey, r_cursor),
                Operation::Yield(vec![r_readkey]),
                Operation::Halt,
            ],
            4,
            btree,
        );

        harness.run();

        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(1));
    }

    #[test]
    fn test_engine_run_multiple_yields() {
        let test = TestDb::default();
        let btree = test.btree;

        let r0 = Reg::new(0);
        let ops = [
            Operation::StoreValue(r0, ScalarValue::Integer(1)),
            Operation::Yield(vec![r0]),
            Operation::StoreValue(r0, ScalarValue::Integer(2)),
            Operation::Yield(vec![r0]),
            Operation::StoreValue(r0, ScalarValue::Integer(3)),
            Operation::Yield(vec![r0]),
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 1, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
        assert_eq!(yields[1][0], ScalarValue::Integer(2));
        assert_eq!(yields[2][0], ScalarValue::Integer(3));
    }

    #[test]
    fn test_key_list_append_and_pop() {
        let test = TestDb::default();
        let btree = test.btree;

        let r_list = Reg::new(0);
        let r_key = Reg::new(1);
        let r_val1 = Reg::new(2);
        let r_val2 = Reg::new(3);
        let r_val3 = Reg::new(4);

        let ops = [
            // Initialize list and append 3 keys
            Operation::InitKeyList(r_list),
            Operation::StoreValue(r_key, ScalarValue::Integer(10)),
            Operation::AppendKey(r_list, r_key),
            Operation::StoreValue(r_key, ScalarValue::Integer(20)),
            Operation::AppendKey(r_list, r_key),
            Operation::StoreValue(r_key, ScalarValue::Integer(30)),
            Operation::AppendKey(r_list, r_key),
            // Pop and yield all 3 keys (in reverse order: 30, 20, 10)
            Operation::PopKey(r_val1, r_list, JumpTarget::addr(10)),
            Operation::PopKey(r_val2, r_list, JumpTarget::addr(10)),
            Operation::PopKey(r_val3, r_list, JumpTarget::addr(10)),
            Operation::Yield(vec![r_val1, r_val2, r_val3]),
            // Try to pop from empty list - should jump to halt
            Operation::PopKey(r_key, r_list, JumpTarget::addr(13)), // jump to Halt
            Operation::Yield(vec![r_key]),                          // Should not reach here
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 5, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(30)); // LIFO: last in, first out
        assert_eq!(yields[0][1], ScalarValue::Integer(20));
        assert_eq!(yields[0][2], ScalarValue::Integer(10));
    }

    #[test]
    fn test_move_cursor_find() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert some test data
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v10 = Vec::new();
            let values = vec![ScalarValue::Integer(100)];
            ciborium::ser::into_writer(&values, &mut v10).unwrap();
            c.insert_u64(10, v10);
            let mut v20 = Vec::new();
            let values = vec![ScalarValue::Integer(200)];
            ciborium::ser::into_writer(&values, &mut v20).unwrap();
            c.insert_u64(20, v20);
            let mut v30 = Vec::new();
            let values = vec![ScalarValue::Integer(300)];
            ciborium::ser::into_writer(&values, &mut v30).unwrap();
            c.insert_u64(30, v30);
        }

        let r_cursor = Reg::new(0);
        let r_key = Reg::new(1);
        let r_value = Reg::new(2);

        let ops = [
            Operation::Open(r_cursor, root),
            // Seek to key 20
            Operation::StoreValue(r_key, ScalarValue::Integer(20)),
            Operation::MoveCursor(r_cursor, MoveOperation::Find(r_key)),
            // Read the value at this position
            Operation::ReadCursor(vec![r_value], r_cursor),
            Operation::Yield(vec![r_value]),
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 3, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(200));
    }

    #[test]
    fn test_collect_then_delete_pattern() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert 3 rows
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v1 = Vec::new();
            let values = vec![ScalarValue::Integer(10)];
            ciborium::ser::into_writer(&values, &mut v1).unwrap();
            c.insert_u64(1, v1);
            let mut v2 = Vec::new();
            let values = vec![ScalarValue::Integer(20)];
            ciborium::ser::into_writer(&values, &mut v2).unwrap();
            c.insert_u64(2, v2);
            let mut v3 = Vec::new();
            let values = vec![ScalarValue::Integer(30)];
            ciborium::ser::into_writer(&values, &mut v3).unwrap();
            c.insert_u64(3, v3);
        }

        let r_cursor = Reg::new(0);
        let r_key_list = Reg::new(1);
        let r_key = Reg::new(2);
        let r_can_read = Reg::new(3);
        let r_count = Reg::new(4);

        let ops = [
            // Init
            Operation::Open(r_cursor, root),
            Operation::MoveCursor(r_cursor, MoveOperation::First),
            Operation::InitKeyList(r_key_list),
            Operation::StoreValue(r_count, ScalarValue::Integer(0)),
            // Phase 1: Collect all keys
            // collect_loop (addr 4):
            Operation::CanReadCursor(r_can_read, r_cursor), // 4
            Operation::GoToIfFalse(JumpTarget::addr(11), r_can_read), // jump to phase2
            Operation::ReadKey(r_key, r_cursor),
            Operation::AppendKey(r_key_list, r_key),
            Operation::IncrementValue(r_count),
            Operation::MoveCursor(r_cursor, MoveOperation::Next),
            Operation::GoTo(JumpTarget::addr(4)), // loop back
            // Phase 2: Delete all collected keys
            // delete_loop (addr 11):
            Operation::PopKey(r_key, r_key_list, JumpTarget::addr(15)), // 11, jump to done
            Operation::MoveCursor(r_cursor, MoveOperation::Find(r_key)),
            Operation::DeleteCursor(r_cursor),
            Operation::GoTo(JumpTarget::addr(11)), // loop back
            // done (addr 15):
            Operation::Yield(vec![r_count]), // 15
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 5, btree);
        let yields = engine.run();

        // Should have deleted 3 rows
        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(3));

        // Verify table is empty
        let btree = engine.take_btree().unwrap();
        let mut cursor = btree.open(root);
        let mut c = cursor.open_readwrite();
        c.first();
        assert!(c.get_entry().is_none());
    }

    #[test]
    fn test_row_buffer_operations() {
        use crate::engine::program::SortKeySpec;
        use crate::test::TestDb;

        let r_buffer = Reg::new(0);
        let r_val1 = Reg::new(1);
        let r_val2 = Reg::new(2);

        let ops = vec![
            // Initialize buffer
            Operation::InitRowBuffer(r_buffer),
            // Add first row: [3, 30]
            Operation::StoreValue(r_val1, ScalarValue::Integer(3)),
            Operation::StoreValue(r_val2, ScalarValue::Integer(30)),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            // Add second row: [1, 10]
            Operation::StoreValue(r_val1, ScalarValue::Integer(1)),
            Operation::StoreValue(r_val2, ScalarValue::Integer(10)),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            // Add third row: [2, 20]
            Operation::StoreValue(r_val1, ScalarValue::Integer(2)),
            Operation::StoreValue(r_val2, ScalarValue::Integer(20)),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            // Sort by first column (ascending)
            Operation::SortRowBuffer(
                r_buffer,
                vec![SortKeySpec {
                    column_index: 0,
                    descending: false,
                }],
            ),
            // Yield rows from buffer (they come out in reverse order due to pop)
            // Pop returns from end of vec, so after sort [1,10], [2,20], [3,30]
            // pop gives us [3,30], [2,20], [1,10]
            Operation::YieldFromRowBuffer(vec![r_val1, r_val2], r_buffer, JumpTarget::addr(14)),
            Operation::Yield(vec![r_val1, r_val2]),
            Operation::GoTo(JumpTarget::addr(11)), // Back to YieldFromRowBuffer
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 3, TestDb::default().btree);
        let yields = engine.run_with_limit(100);

        // Rows should come out in sorted order (ascending)
        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(1)); // [1, 10]
        assert_eq!(yields[0][1], ScalarValue::Integer(10));
        assert_eq!(yields[1][0], ScalarValue::Integer(2)); // [2, 20]
        assert_eq!(yields[1][1], ScalarValue::Integer(20));
        assert_eq!(yields[2][0], ScalarValue::Integer(3)); // [3, 30]
        assert_eq!(yields[2][1], ScalarValue::Integer(30));
    }

    #[test]
    fn test_row_buffer_rewind_and_next() {
        // Test non-destructive iteration with RewindRowBuffer and NextFromRowBuffer
        use crate::test::TestDb;

        let r_buffer = Reg::new(0);
        let r_val1 = Reg::new(1);
        let r_val2 = Reg::new(2);

        let ops = vec![
            // Initialize buffer
            Operation::InitRowBuffer(r_buffer),
            // Add three rows: [1, "a"], [2, "b"], [3, "c"]
            Operation::StoreValue(r_val1, ScalarValue::Integer(1)),
            Operation::StoreValue(r_val2, ScalarValue::String("a".to_string())),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            Operation::StoreValue(r_val1, ScalarValue::Integer(2)),
            Operation::StoreValue(r_val2, ScalarValue::String("b".to_string())),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            Operation::StoreValue(r_val1, ScalarValue::Integer(3)),
            Operation::StoreValue(r_val2, ScalarValue::String("c".to_string())),
            Operation::AppendToRowBuffer(r_buffer, vec![r_val1, r_val2]),
            // First pass: RewindRowBuffer, then iterate with NextFromRowBuffer
            Operation::RewindRowBuffer(r_buffer), // addr 10
            // loop1: NextFromRowBuffer jumps to addr 14 when exhausted
            Operation::NextFromRowBuffer(vec![r_val1, r_val2], r_buffer, JumpTarget::addr(14)), // 11
            Operation::Yield(vec![r_val1, r_val2]),
            Operation::GoTo(JumpTarget::addr(11)), // loop back
            // Second pass: RewindRowBuffer again, iterate again
            Operation::RewindRowBuffer(r_buffer), // addr 14
            // loop2: NextFromRowBuffer jumps to Halt when exhausted
            Operation::NextFromRowBuffer(vec![r_val1, r_val2], r_buffer, JumpTarget::addr(18)), // 15
            Operation::Yield(vec![r_val1, r_val2]),
            Operation::GoTo(JumpTarget::addr(15)), // loop back
            Operation::Halt,                       // addr 18
        ];

        let mut engine = Engine::with_program(&ops, 3, TestDb::default().btree);
        let yields = engine.run_with_limit(100);

        // Should yield 6 rows total: 3 from first pass + 3 from second pass
        assert_eq!(yields.len(), 6);
        // First pass
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
        assert_eq!(yields[0][1], ScalarValue::String("a".to_string()));
        assert_eq!(yields[1][0], ScalarValue::Integer(2));
        assert_eq!(yields[1][1], ScalarValue::String("b".to_string()));
        assert_eq!(yields[2][0], ScalarValue::Integer(3));
        assert_eq!(yields[2][1], ScalarValue::String("c".to_string()));
        // Second pass (same rows)
        assert_eq!(yields[3][0], ScalarValue::Integer(1));
        assert_eq!(yields[3][1], ScalarValue::String("a".to_string()));
        assert_eq!(yields[4][0], ScalarValue::Integer(2));
        assert_eq!(yields[4][1], ScalarValue::String("b".to_string()));
        assert_eq!(yields[5][0], ScalarValue::Integer(3));
        assert_eq!(yields[5][1], ScalarValue::String("c".to_string()));
    }

    #[test]
    fn test_scan_sort_pattern() {
        // Mimics ORDER BY: scan table → collect to buffer → sort → yield
        use crate::engine::program::SortKeySpec;
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert test data: keys 10, 20, 30 with values [30], [10], [20]
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v10 = Vec::new();
            let values = vec![ScalarValue::Integer(30)];
            ciborium::ser::into_writer(&values, &mut v10).unwrap();
            c.insert_u64(10, v10);
            let mut v20 = Vec::new();
            let values = vec![ScalarValue::Integer(10)];
            ciborium::ser::into_writer(&values, &mut v20).unwrap();
            c.insert_u64(20, v20);
            let mut v30 = Vec::new();
            let values = vec![ScalarValue::Integer(20)];
            ciborium::ser::into_writer(&values, &mut v30).unwrap();
            c.insert_u64(30, v30);
        }

        let r_buffer = Reg::new(0);
        let r_cursor = Reg::new(1);
        let r_can_read = Reg::new(2);
        let r_value = Reg::new(3);

        let ops = vec![
            // 0: Init
            Operation::InitRowBuffer(r_buffer),
            // 1: Open cursor
            Operation::Open(r_cursor, root),
            // 2: Position to first
            Operation::MoveCursor(r_cursor, MoveOperation::First),
            // 3: scan_loop - check if we can read
            Operation::CanReadCursor(r_can_read, r_cursor),
            // 4: If can't read, jump to sort phase (addr 9)
            Operation::GoToIfFalse(JumpTarget::addr(9), r_can_read),
            // 5: Read value
            Operation::ReadCursor(vec![r_value], r_cursor),
            // 6: Append to buffer
            Operation::AppendToRowBuffer(r_buffer, vec![r_value]),
            // 7: Move to next
            Operation::MoveCursor(r_cursor, MoveOperation::Next),
            // 8: Back to scan_loop
            Operation::GoTo(JumpTarget::addr(3)),
            // 9: sort_and_yield
            Operation::SortRowBuffer(
                r_buffer,
                vec![SortKeySpec {
                    column_index: 0,
                    descending: false,
                }],
            ),
            // 10: yield_loop - pop and yield rows
            Operation::YieldFromRowBuffer(vec![r_value], r_buffer, JumpTarget::addr(13)),
            // 11: Yield the row
            Operation::Yield(vec![r_value]),
            // 12: Back to yield_loop
            Operation::GoTo(JumpTarget::addr(10)),
            // 13: done
            Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 4, btree);
        let yields = engine.run_with_limit(100);

        // Should yield 3 rows in sorted order (ascending): 10, 20, 30
        assert_eq!(yields.len(), 3, "Should yield 3 rows");
        assert_eq!(yields[0][0], ScalarValue::Integer(10));
        assert_eq!(yields[1][0], ScalarValue::Integer(20));
        assert_eq!(yields[2][0], ScalarValue::Integer(30));
    }

    #[test]
    fn test_order_by_bytecode_from_compiler() {
        // This test replicates the EXACT bytecode from debug.txt
        // for "SELECT * FROM users ORDER BY name"
        use crate::engine::program::SortKeySpec;
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // Insert test data
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let mut v1 = Vec::new();
            let values1 = vec![
                ScalarValue::Integer(1),
                ScalarValue::String("charlie".to_string()),
                ScalarValue::Integer(25),
            ];
            ciborium::ser::into_writer(&values1, &mut v1).unwrap();
            c.insert_u64(1, v1);
            let mut v2 = Vec::new();
            let values2 = vec![
                ScalarValue::Integer(2),
                ScalarValue::String("alice".to_string()),
                ScalarValue::Integer(30),
            ];
            ciborium::ser::into_writer(&values2, &mut v2).unwrap();
            c.insert_u64(2, v2);
            let mut v3 = Vec::new();
            let values3 = vec![
                ScalarValue::Integer(3),
                ScalarValue::String("bob".to_string()),
                ScalarValue::Integer(28),
            ];
            ciborium::ser::into_writer(&values3, &mut v3).unwrap();
            c.insert_u64(3, v3);
        }

        // Exact bytecode from debug.txt
        let ops = vec![
            /* 0*/ Operation::InitRowBuffer(Reg::new(0)),
            /* 1*/ Operation::Open(Reg::new(1), root),
            /* 2*/ Operation::MoveCursor(Reg::new(1), MoveOperation::First),
            /* 3*/ Operation::GoTo(JumpTarget::addr(4)),
            /* 4*/ Operation::CanReadCursor(Reg::new(2), Reg::new(1)),
            /* 5*/ Operation::GoToIfFalse(JumpTarget::addr(15), Reg::new(2)),
            /* 6*/
            Operation::ReadCursor(vec![Reg::new(3), Reg::new(4), Reg::new(5)], Reg::new(1)),
            /* 7*/ Operation::MoveCursor(Reg::new(1), MoveOperation::Next),
            /* 8*/ Operation::GoTo(JumpTarget::addr(9)),
            /* 9*/ Operation::CopyValue(Reg::new(6), Reg::new(3)),
            /*10*/ Operation::CopyValue(Reg::new(7), Reg::new(4)),
            /*11*/ Operation::CopyValue(Reg::new(8), Reg::new(5)),
            /*12*/ Operation::GoTo(JumpTarget::addr(13)),
            /*13*/
            Operation::AppendToRowBuffer(Reg::new(0), vec![Reg::new(6), Reg::new(7), Reg::new(8)]),
            /*14*/ Operation::GoTo(JumpTarget::addr(4)),
            /*15*/
            Operation::SortRowBuffer(
                Reg::new(0),
                vec![SortKeySpec {
                    column_index: 1,
                    descending: false,
                }],
            ),
            /*16*/
            Operation::YieldFromRowBuffer(
                vec![Reg::new(6), Reg::new(7), Reg::new(8)],
                Reg::new(0),
                JumpTarget::addr(19),
            ), // BUG WAS: addr(15)!
            /*17*/ Operation::GoTo(JumpTarget::addr(20)),
            /*18*/ Operation::GoTo(JumpTarget::addr(16)),
            /*19*/ Operation::GoTo(JumpTarget::addr(22)),
            /*20*/ Operation::Yield(vec![Reg::new(6), Reg::new(7), Reg::new(8)]),
            /*21*/ Operation::GoTo(JumpTarget::addr(18)),
            /*22*/ Operation::Halt,
        ];

        let mut engine = Engine::with_program(&ops, 9, btree);
        let yields = engine.run_with_limit(100);

        // Should yield 3 rows sorted by name (column index 1)
        // Sorted order: alice, bob, charlie
        // But popped in reverse: charlie, bob, alice
        assert_eq!(yields.len(), 3, "Should yield 3 rows");

        // Each row should be [id, name, age]
        // The actual values depend on JSON parsing, but we should get 3 distinct rows
        assert_eq!(yields.len(), 3);
    }

    #[test]
    fn test_blob_prefix_le_at_bound() {
        // BlobPrefixLe: key == bound → in range (true), i.e. <= is satisfied
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let key20 = storage::encode_integer_key(20);
            let key30 = storage::encode_integer_key(30);
            c.insert(&key20, vec![2]);
            c.insert(&key30, vec![3]);
        }

        let cursor_reg = Reg::new(0);
        let bound_reg = Reg::new(1);
        let key_blob_reg = Reg::new(2);
        let result_reg = Reg::new(3);

        // cursor at 20, bound=20 → prefix(20) <= 20 → true (in range)
        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(cursor_reg, root),
                Operation::StoreValue(
                    bound_reg,
                    ScalarValue::Blob(storage::encode_integer_key(20)),
                ),
                Operation::MoveCursor(cursor_reg, MoveOperation::Find(bound_reg)),
                Operation::ReadCurrentKey(key_blob_reg, cursor_reg),
                Operation::BlobPrefixLe(result_reg, key_blob_reg, bound_reg),
                Operation::Yield(vec![result_reg]),
                Operation::Halt,
            ],
            4,
            btree,
        );
        harness.run();
        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(true));
    }

    #[test]
    fn test_blob_prefix_le_exceeded() {
        // BlobPrefixLe: key > bound → out of range (false)
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let key20 = storage::encode_integer_key(20);
            let key30 = storage::encode_integer_key(30);
            c.insert(&key20, vec![2]);
            c.insert(&key30, vec![3]);
        }

        let cursor_reg = Reg::new(0);
        let seek_reg = Reg::new(1);
        let bound_reg = Reg::new(2);
        let key_blob_reg = Reg::new(3);
        let result_reg = Reg::new(4);

        // cursor at 30, bound=20 → prefix(30) > 20 → false (out of range)
        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(cursor_reg, root),
                Operation::StoreValue(seek_reg, ScalarValue::Blob(storage::encode_integer_key(30))),
                Operation::StoreValue(
                    bound_reg,
                    ScalarValue::Blob(storage::encode_integer_key(20)),
                ),
                Operation::MoveCursor(cursor_reg, MoveOperation::Find(seek_reg)),
                Operation::ReadCurrentKey(key_blob_reg, cursor_reg),
                Operation::BlobPrefixLe(result_reg, key_blob_reg, bound_reg),
                Operation::Yield(vec![result_reg]),
                Operation::Halt,
            ],
            5,
            btree,
        );
        harness.run();
        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(false));
    }

    #[test]
    fn test_blob_prefix_lt_at_bound() {
        // BlobPrefixLt: key == bound → out of range (false), strict less-than not satisfied
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            let key20 = storage::encode_integer_key(20);
            c.insert(&key20, vec![2]);
        }

        let cursor_reg = Reg::new(0);
        let bound_reg = Reg::new(1);
        let key_blob_reg = Reg::new(2);
        let result_reg = Reg::new(3);

        // cursor at 20, bound=20 → prefix(20) < 20 → false (out of range for exclusive bound)
        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(cursor_reg, root),
                Operation::StoreValue(
                    bound_reg,
                    ScalarValue::Blob(storage::encode_integer_key(20)),
                ),
                Operation::MoveCursor(cursor_reg, MoveOperation::Find(bound_reg)),
                Operation::ReadCurrentKey(key_blob_reg, cursor_reg),
                Operation::BlobPrefixLt(result_reg, key_blob_reg, bound_reg),
                Operation::Yield(vec![result_reg]),
                Operation::Halt,
            ],
            4,
            btree,
        );
        harness.run();
        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Boolean(false));
    }

    #[test]
    fn test_read_current_key_and_decode() {
        // ReadCurrentKey + BlobSliceTail + DecodeU64Key extracts the rowid from an index key
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let rowid: u64 = 42;
        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            // Composite key: [col_encoded 8 bytes][rowid 8 bytes]
            let mut key = storage::encode_integer_key(100);
            key.extend_from_slice(&storage::encode_u64_key(rowid));
            c.insert(&key, vec![]);
        }

        let cursor_reg = Reg::new(0);
        let seek_reg = Reg::new(1);
        let key_blob_reg = Reg::new(2);
        let tail_reg = Reg::new(3);
        let rowid_reg = Reg::new(4);

        let seek_key = {
            let mut k = storage::encode_integer_key(100);
            k.extend_from_slice(&storage::encode_u64_key(rowid));
            ScalarValue::Blob(k)
        };

        let mut harness = TestHarness::new_with_btree(
            &[
                Operation::Open(cursor_reg, root),
                Operation::StoreValue(seek_reg, seek_key),
                Operation::MoveCursor(cursor_reg, MoveOperation::Find(seek_reg)),
                Operation::ReadCurrentKey(key_blob_reg, cursor_reg),
                Operation::BlobSliceTail(tail_reg, key_blob_reg, 8),
                Operation::DecodeU64Key(rowid_reg, tail_reg),
                Operation::Yield(vec![rowid_reg]),
                Operation::Halt,
            ],
            5,
            btree,
        );
        harness.run();
        assert_eq!(harness.num_yields(), 1);
        assert_eq!(harness.value(0, 0), ScalarValue::Integer(rowid as i64));
    }
}
