use std::{
    mem,
    path::PathBuf,
    sync::{Arc, Once, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use super::Catalog;
use crate::{
    btree::buffer_pool::BufferPool,
    transaction::{ConcurrentStatus, LogManager},
    types::Pod,
    utils::HandyRwLock,
};

/// We collect all global variables here.
///
/// These variable cannot be initialized as static variables, because
/// their initialization function all rely on non-const fn (e.g.
/// `HashMap::new()`).
///
/// In the same time, all these variables should not be wrapped in any
/// kind of smark pointers / locks (e.g. `Arc`, `RwLock`), because
/// they are used in concurrent environment, and it's hard, if not
/// impossible, to acquire a exclusive lock in any context.
///
/// TODO: update this comment
///
/// TODO: support multiple databases
pub struct Database {
    path: PathBuf,

    buffer_pool: Pod<BufferPool>,
    catalog: Pod<Catalog>,
    concurrent_status: Pod<ConcurrentStatus>,
    log_manager: Pod<LogManager>,
}

static mut SINGLETON: *mut Database = 0 as *mut Database;

impl Database {
    fn new() -> Self {
        let db_name = "default_db";
        let db_path = PathBuf::from("data").join(db_name);
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path).unwrap();
        }

        let log_path = db_path.join("wal.log");

        let instance = Self {
            path: db_path,

            buffer_pool: Arc::new(RwLock::new(BufferPool::new())),
            concurrent_status: Arc::new(RwLock::new(ConcurrentStatus::new())),
            catalog: Arc::new(RwLock::new(Catalog::new())),
            log_manager: Arc::new(RwLock::new(LogManager::new(log_path))),
        };

        return instance;
    }

    /// Reset the memory status of the database, used for tests
    /// mostly.
    ///
    /// Actions:
    /// - Page cache will be cleared.
    /// - Catalog will be cleared.
    /// - Status of `log_manager` will be reset, but the log file
    ///  itself will keep unchanged.
    pub fn reset() {
        // Initialize the new db instance.
        let singleton = Self::new();

        unsafe {
            if !SINGLETON.is_null() {
                // Drop the previous db instance if it's already
                // initialized.
                mem::drop(Box::from_raw(SINGLETON));
            }

            // Put it in the heap so it can outlive this call.
            SINGLETON = mem::transmute(Box::new(singleton));
        }

        Catalog::load_tables().unwrap();
        Catalog::load_schemas().unwrap();

        Database::mut_log_manager().recover().unwrap();
        Database::mut_concurrent_status().clear();
    }

    pub fn mut_buffer_pool() -> RwLockWriteGuard<'static, BufferPool> {
        Self::global().buffer_pool.wl()
    }

    pub(crate) fn concurrent_status() -> RwLockReadGuard<'static, ConcurrentStatus> {
        Self::global().concurrent_status.rl()
    }

    pub(crate) fn mut_concurrent_status() -> RwLockWriteGuard<'static, ConcurrentStatus> {
        Self::global().concurrent_status.wl()
    }

    pub fn catalog() -> RwLockReadGuard<'static, Catalog> {
        Self::global().catalog.rl()
    }

    pub fn mut_catalog() -> RwLockWriteGuard<'static, Catalog> {
        Self::global().catalog.wl()
    }

    pub fn log_manager() -> RwLockReadGuard<'static, LogManager> {
        Self::global().log_manager.rl()
    }

    pub fn mut_log_manager() -> RwLockWriteGuard<'static, LogManager> {
        Self::global().log_manager.wl()
    }

    pub fn global() -> &'static Self {
        // Initialize it to a null value
        // static mut SINGLETON: *mut Database = 0 as *mut Database;
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

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }
}
