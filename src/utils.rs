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

use crate::{
    btree::buffer_pool::BufferPool, concurrent_status::ConcurrentStatus,
    types::Pod, Catalog,
};
pub use crate::{btree::tuple::simple_int_tuple_scheme, log::init_log};

pub fn lock_state<T>(lock: impl Deref<Target = RwLock<T>>) -> String {
    let is_read: bool = lock.try_read().is_err();
    let is_write: bool = lock.try_write().is_err();
    let is_poisoned: bool = lock.is_poisoned();
    format!("[r: {}, w: {}, p: {}]", is_read, is_write, is_poisoned)
}

pub struct Unique {
    buffer_pool: Pod<BufferPool>,
    catalog: Pod<Catalog>,
    concurrent_status: Pod<ConcurrentStatus>,
}

impl Unique {
    fn new() -> Self {
        Self {
            buffer_pool: Arc::new(RwLock::new(BufferPool::new())),
            catalog: Arc::new(RwLock::new(Catalog::new())),
            concurrent_status: Arc::new(RwLock::new(ConcurrentStatus::new())),
        }
    }

    pub fn buffer_pool() -> RwLockReadGuard<'static, BufferPool> {
        Self::global().buffer_pool.rl()
    }

    // We should not request for a writeable buffer pool for ever, for the
    // buffer pool is a concurrent data structure.
    // pub fn buffer_pool() -> RwLockWriteGuard<'static, BufferPool> {
    //     Self::global().buffer_pool.wl()
    // }

    pub fn catalog() -> RwLockReadGuard<'static, Catalog> {
        Self::global().catalog.rl()
    }

    pub fn mut_catalog() -> RwLockWriteGuard<'static, Catalog> {
        Self::global().catalog.wl()
    }

    pub fn concurrent_status() -> RwLockReadGuard<'static, ConcurrentStatus> {
        Self::global().concurrent_status.rl()
    }

    pub fn mut_concurrent_status() -> RwLockWriteGuard<'static, ConcurrentStatus>
    {
        Self::global().concurrent_status.wl()
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
