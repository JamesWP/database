use super::program::{ScalarValue, Reg};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterValue {
    None,
    ScalarValue(ScalarValue)
}

#[derive(Clone, Debug)]
pub struct Registers {
    file: Vec<RegisterValue>
}

pub struct RegisterIterator<'a, RegIter: Iterator<Item=&'a Reg>> {
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
        match self {
            RegisterValue::ScalarValue(ref mut s) => Some(s),
            _ => None,
        }
    }
}

impl<'a, RegIter: Iterator<Item=&'a Reg>> Iterator for RegisterIterator<'a, RegIter> {
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

    pub fn get_range<'a>(&'a self, regs: &'a [Reg]) -> RegisterIterator<'a, core::slice::Iter<'a, Reg>> {
        RegisterIterator { values: &self.file, regs: regs.iter()}
    }

    pub(crate) fn new(size: usize) -> Registers {
        let mut file = Vec::with_capacity(size);

        for _ in 0..size {
            file.push(RegisterValue::default());
        }

        Registers { file }
    }
}