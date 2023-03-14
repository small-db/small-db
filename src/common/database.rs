use std::{
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Once, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use super::Catalog;
use crate::{
    btree::page_cache::PageCache,
    concurrent_status::ConcurrentStatus, tx_log::LogManager,
    types::Pod, utils::HandyRwLock,
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

    buffer_pool: Pod<PageCache>,
    catalog: Pod<Catalog>,
    concurrent_status: ConcurrentStatus,
    log_file: Pod<LogManager>,
}

static mut SINGLETON: *mut Database = 0 as *mut Database;

impl Database {
    fn new() -> Self {
        let db_name = "default_db";
        let path = PathBuf::from("data").join(db_name);
        if !path.exists() {
            std::fs::create_dir_all(&path).unwrap();
        }

        Self {
            path,

            buffer_pool: Arc::new(RwLock::new(PageCache::new())),
            concurrent_status: ConcurrentStatus::new(),
            catalog: Arc::new(RwLock::new(Catalog::new())),
            log_file: Arc::new(RwLock::new(LogManager::new(
                "wal.log",
            ))),
        }
    }

    /// Reset the database, used for unit tests only.
    pub fn reset() {
        mem::drop(unsafe { Box::from_raw(SINGLETON) });

        // Make it
        let singleton = Self::new();

        unsafe {
            // Put it in the heap so it can outlive this call
            SINGLETON = mem::transmute(Box::new(singleton));
        }

        let catalog_file_path = Path::new(&Self::global().path)
            .join("catalog")
            .to_str()
            .unwrap()
            .to_string();
        Self::mut_catalog().load_schema(&catalog_file_path);
    }

    pub fn mut_page_cache() -> RwLockWriteGuard<'static, PageCache> {
        Self::global().buffer_pool.wl()
    }

    pub fn buffer_pool_pod() -> Arc<RwLock<PageCache>> {
        Self::global().buffer_pool.clone()
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

    pub fn mut_log_manager() -> RwLockWriteGuard<'static, LogManager>
    {
        Self::global().log_file.wl()
    }

    pub fn log_file_pod() -> Arc<RwLock<LogManager>> {
        Self::global().log_file.clone()
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
}
