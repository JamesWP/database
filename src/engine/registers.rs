use super::program::{ScalarValue, Reg};

#[derive(Clone, Debug)]
pub enum RegisterValue {
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

impl RegisterValue {
    pub fn scalar(&self) -> Option<&ScalarValue> {
        match self {
            RegisterValue::ScalarValue(s) => Some(s),
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
}