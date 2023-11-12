use crate::storage::CursorHandle;

use super::{
    program::Reg,
    scalarvalue::{self, ScalarValue},
};

#[derive(Clone, Debug)]
pub enum RegisterValue {
    None,
    ScalarValue(ScalarValue),
    CursorHandle(CursorHandle),
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

    pub(crate) fn scalar_mut(&mut self) -> Option<&mut ScalarValue> {
        if let RegisterValue::ScalarValue(ref mut s) = self {
            Some(s)
        } else {
            None
        }
    }

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
