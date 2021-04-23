use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

// copy from https://github.com/tikv/tikv/blob/b15ea3b1cd766375cb52019e35c195ed797124df/components/tikv_util/src/lib.rs#L171-L186
// / A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
// / shortcut wl and rl.
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T>;
    fn rl(&self) -> RwLockReadGuard<'_, T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap()
    }

    fn rl(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap()
    }
}

pub use crate::tuple::simple_int_tuple_scheme;
pub use crate::log::init_log;