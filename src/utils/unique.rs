use std::{
    mem,
    sync::{Arc, Once, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use super::HandyRwLock;
use crate::{
    btree::buffer_pool::BufferPool, concurrent_status::ConcurrentStatus,
    tx_log::LogManager, types::Pod, Catalog,
};

/// We collect all global variables here.
///
/// These variable cannot be initialized as static variables, because their
/// initialization function all rely on non-const fn (e.g. `HashMap::new()`).
///
/// In the same time, all these variables should not be wrapped in any kind of
/// smark pointers / locks (e.g. `Arc`, `RwLock`), because they are used in
/// concurrent environment, and it's hard, if not impossible, to acquire a
/// exclusive lock in any context.
pub struct Unique {
    buffer_pool: BufferPool,
    catalog: Pod<Catalog>,
    concurrent_status: ConcurrentStatus,
    log_file: Pod<LogManager>,
}

impl Unique {
    fn new() -> Self {
        Self {
            buffer_pool: BufferPool::new(),
            concurrent_status: ConcurrentStatus::new(),
            catalog: Arc::new(RwLock::new(Catalog::new())),
            log_file: Arc::new(RwLock::new(LogManager::new("wal.log"))),
        }
    }

    pub fn buffer_pool() -> &'static BufferPool {
        &Self::global().buffer_pool
    }

    pub fn concurrent_status() -> &'static ConcurrentStatus {
        &Self::global().concurrent_status
    }

    pub fn catalog() -> RwLockReadGuard<'static, Catalog> {
        Self::global().catalog.rl()
    }

    pub fn mut_catalog() -> RwLockWriteGuard<'static, Catalog> {
        Self::global().catalog.wl()
    }

    pub fn log_file() -> RwLockReadGuard<'static, LogManager> {
        Self::global().log_file.rl()
    }

    pub fn mut_log_file() -> RwLockWriteGuard<'static, LogManager> {
        Self::global().log_file.wl()
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