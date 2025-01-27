use std::{
    mem::ManuallyDrop,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

pub union AtomicUnion {
    high: ManuallyDrop<AtomicU64>,
    _low: ManuallyDrop<AtomicU32>,
}

impl AtomicUnion {
    #[allow(dead_code)]
    pub fn fetch_add_high(&self, val: u32, ord: Ordering) -> (u32, u32) {
        let raw = unsafe { self.high.fetch_add((val as u64) << 32, ord) };
        let low = (raw & (u32::MAX as u64)) as u32;
        let high = (raw >> 32) as u32;

        (low, high)
    }

    #[allow(dead_code)]
    pub fn fetch_add_high_low(&self, val_high: u32, val_low: u32, ord: Ordering) -> (u32, u32) {
        let val = ((val_high as u64) << 32) + val_low as u64;
        let raw = unsafe { self.high.fetch_add(val, ord) };
        let low = (raw & (u32::MAX as u64)) as u32;
        let high = (raw >> 32) as u32;

        (low, high)
    }

    #[allow(dead_code)]
    pub fn fetch_add_low(&self, val: u32, ord: Ordering) -> (u32, u32) {
        let raw = unsafe { self.high.fetch_add(val as u64, ord) };
        let low = (raw & (u32::MAX as u64)) as u32;
        let high = (raw >> 32) as u32;

        (low, high)
    }

    #[allow(dead_code)]
    pub fn load_low(&self, ord: Ordering) -> u32 {
        unsafe { self._low.load(ord) }
    }

    #[allow(dead_code)]
    pub fn load_high(&self, ord: Ordering) -> u32 {
        unsafe { (self.high.load(ord) >> 32) as u32 }
    }
}
