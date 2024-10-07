use std::{
    io::prelude::*,
    ops::Deref,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// copy from https://github.com/tikv/tikv/blob/b15ea3b1cd766375cb52019e35c195ed797124df/components/tikv_util/src/lib.rs#L171-L186
///
/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern
/// to shortcut wl and rl.
pub trait HandyRwLock<T: ?Sized> {
    fn wl(&self) -> RwLockWriteGuard<'_, T>;
    fn rl(&self) -> RwLockReadGuard<'_, T>;
}

impl<T: ?Sized> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T> {
        return self.write().unwrap();
        // try to get the lock using exponential backoff

        // let sleep = 10;
        // loop {
        //     match self.try_write() {
        //         Ok(guard) => return guard,
        //         Err(_) => std::thread::sleep(std::time::Duration::from_micros(sleep)),
        //     }
        //     // sleep *= 2;
        // }
    }

    fn rl(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap()
    }
}

pub fn lock_state<T>(lock: impl Deref<Target = RwLock<T>>) -> String {
    let is_read: bool = lock.try_read().is_err();
    let is_write: bool = lock.try_write().is_err();
    let is_poisoned: bool = lock.is_poisoned();
    format!("[r: {}, w: {}, p: {}]", is_read, is_write, is_poisoned)
}
