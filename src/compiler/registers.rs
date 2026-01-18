use crate::engine::program::Reg;

/// A simple bump allocator for VM registers.
/// Allocates registers sequentially starting from 0.
pub struct RegisterAllocator {
    next_reg: usize,
}

impl RegisterAllocator {
    pub fn new() -> Self {
        RegisterAllocator { next_reg: 0 }
    }

    /// Allocate a single register and return it.
    pub fn alloc(&mut self) -> Reg {
        let reg = Reg::new(self.next_reg);
        self.next_reg += 1;
        reg
    }

    /// Allocate a contiguous block of n registers and return them as a Vec.
    pub fn alloc_block(&mut self, n: usize) -> Vec<Reg> {
        let mut regs = Vec::with_capacity(n);
        for _ in 0..n {
            regs.push(self.alloc());
        }
        regs
    }

    /// Return the total number of registers allocated so far.
    pub fn count(&self) -> usize {
        self.next_reg
    }
}

impl Default for RegisterAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_allocation() {
        let mut allocator = RegisterAllocator::new();

        let r0 = allocator.alloc();
        let r1 = allocator.alloc();
        let r2 = allocator.alloc();

        assert_eq!(r0.index(), 0);
        assert_eq!(r1.index(), 1);
        assert_eq!(r2.index(), 2);
        assert_eq!(allocator.count(), 3);
    }

    #[test]
    fn test_block_allocation() {
        let mut allocator = RegisterAllocator::new();

        let block = allocator.alloc_block(5);

        assert_eq!(block.len(), 5);
        assert_eq!(block[0].index(), 0);
        assert_eq!(block[1].index(), 1);
        assert_eq!(block[2].index(), 2);
        assert_eq!(block[3].index(), 3);
        assert_eq!(block[4].index(), 4);
        assert_eq!(allocator.count(), 5);
    }

    #[test]
    fn test_mixed_allocation() {
        let mut allocator = RegisterAllocator::new();

        let r0 = allocator.alloc();
        let block = allocator.alloc_block(3);
        let r4 = allocator.alloc();

        assert_eq!(r0.index(), 0);
        assert_eq!(block[0].index(), 1);
        assert_eq!(block[1].index(), 2);
        assert_eq!(block[2].index(), 3);
        assert_eq!(r4.index(), 4);
        assert_eq!(allocator.count(), 5);
    }

    #[test]
    fn test_empty_block() {
        let mut allocator = RegisterAllocator::new();

        let block = allocator.alloc_block(0);

        assert_eq!(block.len(), 0);
        assert_eq!(allocator.count(), 0);
    }

    #[test]
    fn test_count_starts_at_zero() {
        let allocator = RegisterAllocator::new();
        assert_eq!(allocator.count(), 0);
    }
}
