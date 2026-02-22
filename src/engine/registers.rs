use crate::storage::CursorHandle;

use super::{program::Reg, scalarvalue::ScalarValue};
use std::collections::BTreeMap;

/// Accumulator for aggregate functions
#[derive(Clone, Debug)]
pub enum Accumulator {
    Count { count: i64 },
    Sum { sum: ScalarValue, count: i64 },
    Avg { sum: ScalarValue, count: i64 },
    Min { value: Option<ScalarValue> },
    Max { value: Option<ScalarValue> },
}

/// Group table: maps group keys to their accumulators
pub type GroupTable = BTreeMap<Vec<ScalarValue>, Vec<Accumulator>>;

/// Row buffer for materializing rows and iterating over them.
///
/// Used by Sort (ORDER BY), Join (hash-loop materialisation), and the
/// collect-then-mutate pattern in DELETE/UPDATE.  The `cursor` field is
/// always the read position; `RewindRowBuffer` resets it to 0 and
/// `NextFromRowBuffer` advances it on each call.
#[derive(Clone, Debug)]
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,
}

#[derive(Clone, Debug)]
pub enum RegisterValue {
    None,
    ScalarValue(ScalarValue),
    CursorHandle(CursorHandle),
    RowBuffer(RowBuffer),
    GroupTable(GroupTable),
}

#[derive(Clone, Debug)]
pub struct Registers {
    file: Vec<RegisterValue>,
}

pub struct RegisterIterator<'a, RegIter: Iterator<Item = &'a Reg>> {
    values: &'a [RegisterValue],
    regs: RegIter,
}

impl Default for RegisterValue {
    fn default() -> Self {
        Self::None
    }
}

impl RegisterValue {
    pub fn scalar(&self) -> Option<&ScalarValue> {
        match self {
            RegisterValue::ScalarValue(s) => Some(s),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn scalar_mut(&mut self) -> Option<&mut ScalarValue> {
        if let RegisterValue::ScalarValue(ref mut s) = self {
            Some(s)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn integer(&self) -> Option<i64> {
        if let RegisterValue::ScalarValue(scalar_value) = self {
            if let ScalarValue::Integer(x) = scalar_value {
                Some(*x)
            } else {
                None
            }
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn integer_mut(&mut self) -> Option<&mut i64> {
        if let RegisterValue::ScalarValue(ref mut scalar_value) = self {
            if let ScalarValue::Integer(ref mut x) = scalar_value {
                Some(x)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn boolean(&self) -> Option<bool> {
        if let RegisterValue::ScalarValue(scalar_value) = self {
            if let ScalarValue::Boolean(x) = scalar_value {
                Some(*x)
            } else {
                None
            }
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub(crate) fn cursor(&self) -> Option<&CursorHandle> {
        match self {
            RegisterValue::CursorHandle(c) => Some(c),
            _ => None,
        }
    }

    pub(crate) fn cursor_mut(&mut self) -> Option<&mut CursorHandle> {
        if let RegisterValue::CursorHandle(ref mut c) = self {
            Some(c)
        } else {
            None
        }
    }

    pub(crate) fn row_buffer_mut(&mut self) -> Option<&mut RowBuffer> {
        if let RegisterValue::RowBuffer(ref mut buffer) = self {
            Some(buffer)
        } else {
            None
        }
    }

    pub(crate) fn group_table_mut(&mut self) -> Option<&mut GroupTable> {
        if let RegisterValue::GroupTable(ref mut table) = self {
            Some(table)
        } else {
            None
        }
    }
}

impl<'a, RegIter: Iterator<Item = &'a Reg>> Iterator for RegisterIterator<'a, RegIter> {
    type Item = &'a RegisterValue;

    fn next(&mut self) -> Option<Self::Item> {
        let r = self.regs.next()?;
        Some(self.values.get(r.index()).unwrap())
    }
}

impl Registers {
    pub fn get_mut(&mut self, reg: Reg) -> &mut RegisterValue {
        self.file.get_mut(reg.index()).unwrap()
    }

    pub fn get(&self, reg: Reg) -> &RegisterValue {
        self.file.get(reg.index()).unwrap()
    }

    pub fn get_range<'a>(
        &'a self,
        regs: &'a [Reg],
    ) -> RegisterIterator<'a, core::slice::Iter<'a, Reg>> {
        RegisterIterator {
            values: &self.file,
            regs: regs.iter(),
        }
    }

    pub(crate) fn new(size: usize) -> Registers {
        let mut file = Vec::with_capacity(size);

        for _ in 0..size {
            file.push(RegisterValue::default());
        }

        Registers { file }
    }
}
