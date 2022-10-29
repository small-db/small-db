use std::{
    io::prelude::*,
    mem,
    ops::Deref,
    sync::{Arc, Once, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// copy from https://github.com/tikv/tikv/blob/b15ea3b1cd766375cb52019e35c195ed797124df/components/tikv_util/src/lib.rs#L171-L186
///
/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
/// shortcut wl and rl.
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

use crate::{btree::buffer_pool::BufferPool, types::Pod};
pub use crate::{btree::tuple::simple_int_tuple_scheme, log::init_log};

pub fn lock_state<T>(lock: impl Deref<Target = RwLock<T>>) -> String {
    let is_read: bool = lock.try_read().is_err();
    let is_write: bool = lock.try_write().is_err();
    let is_poisoned: bool = lock.is_poisoned();
    format!("[r: {}, w: {}, p: {}]", is_read, is_write, is_poisoned)
}

pub struct Unique {
    pub buffer_pool: Pod<BufferPool>,
}

impl Unique {
    fn new() -> Self {
        Self {
            buffer_pool: Arc::new(RwLock::new(BufferPool::new())),
        }
    }

    pub fn buffer_pool() -> RwLockReadGuard<'static, BufferPool> {
        Self::global().buffer_pool.rl()
    }

    pub fn mut_buffer_pool() -> RwLockWriteGuard<'static, BufferPool> {
        Self::global().buffer_pool.wl()
    }

    pub fn global() -> &'static Self {
        // Initialize it to a null value
        static mut SINGLETON: *mut Unique = 0 as *mut Unique;
        static ONCE: Once = Once::new();

        ONCE.call_once(|| {
            // Make it
            let singleton = Self::new();

            unsafe {
                // Put it in the heap so it can outlive this call
                SINGLETON = mem::transmute(Box::new(singleton));
            }
        });

        unsafe {
            // Now we give out a copy of the data that is safe to use
            // concurrently.
            SINGLETON.as_ref().unwrap()
        }
    }
}
