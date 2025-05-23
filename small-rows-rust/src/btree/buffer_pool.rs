use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, prelude::*, Seek, SeekFrom},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use super::page::{
    BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePage, BTreePageID,
    BTreeRootPointerPage, PageCategory,
};
use crate::{
    error::SmallError,
    transaction::{ConcurrentStatus, LogManager, Permission, Transaction},
    types::ResultPod,
    utils::HandyRwLock,
    BTreeTable, Database, TableSchema,
};

pub const DEFAULT_PAGE_SIZE: usize = 4096;
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_SIZE);

pub struct BufferPool {
    pub root_pointer_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeRootPointerPage>>>,
    pub internal_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeInternalPage>>>,
    pub leaf_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeLeafPage>>>,
    pub header_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeHeaderPage>>>,

    pub bufferfixed: HashSet<BTreePageID>,
}

type Key = BTreePageID;

impl BufferPool {
    pub fn new() -> Self {
        BufferPool::set_page_size(DEFAULT_PAGE_SIZE);

        Self {
            root_pointer_buffer: HashMap::new(),
            header_buffer: HashMap::new(),
            internal_buffer: HashMap::new(),
            leaf_buffer: HashMap::new(),

            bufferfixed: HashSet::new(),
        }
    }

    pub fn clear(&mut self) {
        self.root_pointer_buffer.clear();
        self.header_buffer.clear();
        self.internal_buffer.clear();
        self.leaf_buffer.clear();
    }

    /// Retrieve the specified page with the associated permissions.
    /// Will acquire a lock and may block if that lock is held by
    /// another transaction.
    ///
    /// The retrieved page should be looked up in the buffer pool.  If
    /// it is present, it should be returned.  If it is not
    /// present, it should be added to the buffer pool and
    /// returned.  If there is insufficient space in the buffer
    /// pool, a page should be evicted and the new page
    /// should be added in its place.
    fn load_page<PAGE>(pid: &Key) -> ResultPod<PAGE>
    where
        PAGE: BTreePage,
    {
        // stage 1: get table
        let mut catalog = Database::mut_catalog();
        let v = catalog
            .get_table(&pid.get_table_id())
            .expect(&format!("table {} not found", pid.get_table_id()));
        let table = v.read().unwrap();

        // stage 2: read page content from disk
        let buf = Self::read_page(&mut table.get_file(), pid)
            .or(Err(SmallError::new("read page content failed")))?;

        // stage 3: page instantiation
        let page = PAGE::new(pid, &buf, &table.schema);

        // stage 4: return
        return Ok(Arc::new(RwLock::new(page)));
    }

    fn read_page(file: &mut File, key: &Key) -> io::Result<Vec<u8>> {
        let page_size = Self::get_page_size();
        let start_pos = key.page_index as usize * page_size;
        file.seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");

        let mut buf: Vec<u8> = vec![0; page_size];
        file.read_exact(&mut buf).expect("io error");
        Ok(buf)
    }

    /// Get a page from the buffer pool, loading it from disk if
    /// necessary.
    ///
    /// Return an error if the page does not exist.
    ///
    /// Method to encapsulate the process of locking/fetching a page. First the
    /// method checks the local cache ("dirtypages"), and if it can't find
    /// the requested page there, it fetches it from the buffer pool.
    /// It also adds pages to the dirtypages cache if they are fetched with
    /// read-write permission, since presumably they will soon be dirtied by
    /// this transaction.
    ///
    /// This method is needed to ensure that page updates are not lost if the
    /// same pages are accessed multiple times.
    fn get_page<PAGE: BTreePage>(
        tx: &Transaction,
        perm: Permission,
        key: &Key,
        get_pool_fn: fn(&mut BufferPool) -> &mut HashMap<Key, Arc<RwLock<PAGE>>>,
    ) -> ResultPod<PAGE> {
        // We need to request lock on the page before access the
        // buffer pool. Here are the reasons:
        //
        // 1. (main reason) Logically, get a page from buffer pool is an access
        //    operation, which requires the permission of the page.
        //
        // 2. If we request the lock on a page after getting the access to the buffer
        //    pool, the request may be blocked by other transactions. But we have
        //    already hold the access to the buffer pool, which leads to deadlock. e.g:
        //    T1: hold page1, request buffer pool (for other pages) T2: hold buffer
        //    pool, request page1 => deadlock
        //
        // 3. The lock scope of buffer pool should be as small as possible, since most
        //    of its operations require exclusive access.

        // step 1: request page latch
        if key.need_page_latch() {
            ConcurrentStatus::request_latch(tx, &perm.to_lock(), key)?;
        }

        // step 2: mark the page as dirty if it is a read-write page
        if perm == Permission::ReadWrite {
            Database::mut_concurrent_status().set_dirty_page(tx, key);
        }

        // step 3: get page from buffer pool
        let mut bp = Database::mut_buffer_pool();
        let pool = get_pool_fn(&mut bp);
        let v = pool.entry(key.clone()).or_insert_with(|| {
            let page = Self::load_page(key).unwrap();
            page
        });
        let page = v.clone();

        return Ok(page);
    }

    pub fn get_root_ptr_page(
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeRootPointerPage> {
        Self::get_page(tx, perm, key, |bp| &mut bp.root_pointer_buffer)
    }

    pub(crate) fn get_header_page(
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeHeaderPage> {
        Self::get_page(tx, perm, key, |bp| &mut bp.header_buffer)
    }

    pub fn get_internal_page(
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeInternalPage> {
        Self::get_page(tx, perm, key, |bp| &mut bp.internal_buffer)
    }

    pub fn get_leaf_page(
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeLeafPage> {
        Self::get_page(tx, perm, key, |bp| &mut bp.leaf_buffer)
    }

    /// Remove the specific page id from the buffer pool.
    /// Needed by the recovery manager to ensure that the
    /// buffer pool doesn't keep a rolled back page in its
    /// cache.
    ///
    /// Also used by B+ tree files to ensure that deleted pages
    /// are removed from the cache so they can be reused safely
    pub(crate) fn discard_page(&mut self, pid: &BTreePageID) {
        match pid.category {
            PageCategory::Internal => {
                self.internal_buffer.remove(pid);
            }
            PageCategory::Leaf => {
                self.leaf_buffer.remove(pid);
            }
            PageCategory::RootPointer => {
                self.root_pointer_buffer.remove(pid);
            }
            PageCategory::Header => {
                self.header_buffer.remove(pid);
            }
        }
    }

    pub fn set_page_size(page_size: usize) {
        PAGE_SIZE.store(page_size, Ordering::Relaxed);
    }

    pub fn get_page_size() -> usize {
        PAGE_SIZE.load(Ordering::Relaxed)
    }

    fn set_before_image<PAGE: BTreePage>(
        &self,
        pid: &BTreePageID,
        buffer: &HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
        table_schema: &TableSchema,
    ) {
        let page_rc = buffer.get(pid).unwrap();
        page_rc.wl().set_before_image(table_schema);
    }

    /// Flush all dirty pages to database.
    pub fn flush_all_pages(&self, log_manager: &mut LogManager) {
        if cfg!(feature = "aries_steal") {
            for pid in self.all_keys() {
                self.flush_page(&pid, log_manager);
            }
        } else if cfg!(feature = "aries_no_steal") {
            // do nothing
            //
            // In NO-STEAL mode, the dirty pages are not allowed to be written
            // to database arbitrarily.
        } else {
            panic!("unknown aries mode");
        }
    }

    /// Write all pages of the specified transaction to disk.
    ///
    /// TODO: remove the "log_manager" parameter
    pub fn flush_pages(&self, tx: &Transaction, log_manager: &mut LogManager) {
        let dirty_pages = Database::concurrent_status().get_dirty_pages(tx);

        // Note: current implementation of the api "flush_page" request
        // "ConcurrentStatus", so we must get "dirty_pages" before the for loop.
        for pid in dirty_pages {
            self.flush_page(&pid, log_manager);
        }
    }

    /// Write all dirty pages of the specified transaction to disk.
    pub(crate) fn write_pages(&self, tx: &Transaction) {
        let dirty_pages = Database::concurrent_status().get_dirty_pages(tx);

        // Note: current implementation of the api "flush_page" request
        // "ConcurrentStatus", so we must get "dirty_pages" before the for loop.
        let mut catalog = Database::mut_catalog();
        for pid in dirty_pages {
            let table_rc = catalog.get_table(&pid.get_table_id()).unwrap();
            let table = table_rc.read().unwrap();

            match pid.category {
                PageCategory::RootPointer => {
                    self.write(&table, &pid, &self.root_pointer_buffer);
                }
                PageCategory::Header => {
                    self.write(&table, &pid, &self.header_buffer);
                }
                PageCategory::Internal => {
                    self.write(&table, &pid, &self.internal_buffer);
                }
                PageCategory::Leaf => {
                    self.write(&table, &pid, &self.leaf_buffer);
                }
            }
        }
    }

    /// Write the content of a specific page to disk.
    fn flush_page(&self, pid: &BTreePageID, log_manager: &mut LogManager) {
        // stage 1: get table
        let table_rc = Database::mut_catalog()
            .get_table(&pid.get_table_id())
            .unwrap();
        let table = table_rc.rl();

        match pid.category {
            PageCategory::RootPointer => {
                self.log_and_write(&table, pid, &self.root_pointer_buffer, log_manager);
            }
            PageCategory::Header => {
                self.log_and_write(&table, pid, &self.header_buffer, log_manager);
            }
            PageCategory::Internal => {
                self.log_and_write(&table, pid, &self.internal_buffer, log_manager);
            }
            PageCategory::Leaf => {
                self.log_and_write(&table, pid, &self.leaf_buffer, log_manager);
            }
        }
    }

    fn log_and_write<PAGE: BTreePage>(
        &self,
        table: &BTreeTable,
        pid: &BTreePageID,
        buffer: &HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
        log_manager: &mut LogManager,
    ) {
        if let Some(page_rc) = buffer.get(pid) {
            table.write_page_to_disk(pid, &page_rc.rl().get_page_data(&table.schema));
            return;

            let v = Database::concurrent_status().dirty_page_tx(pid);
            if let Some(tx) = v {
                log_manager.log_update(&tx, page_rc.clone()).unwrap();

                if cfg!(feature = "aries_force") {
                    table.write_page_to_disk(pid, &page_rc.rl().get_page_data(&table.schema));
                }

                // What's the purpose of "set_before_image" here?
                self.set_before_image(&pid, &buffer, &table.schema);
                return;
            } else {
                // Not a dirty page, so no need to write to log or disk, just return.
                //
                // Q: What's the possiable scenario for this case?
                // A: This happens when "flass_all_pages" is called, and the some pages
                // are not dirty.
                return;
            }
        } else {
            // Page not found in buffer pool, so no need to write to disk. This happens
            // when a page is deleted during the transaction.
            //
            // Q: What's the possiable scenario for this case?
            // A: For example, when a transaction deletes some tuples, may cause a leaf page
            // to be empty and be discarded from the buffer pool. But the page
            // is still recorded in the relationship map.
            //
            // TODO: remove the page from the relationship map when the page is discarded.
            return;
        }
    }

    fn write<PAGE: BTreePage>(
        &self,
        table: &BTreeTable,
        pid: &BTreePageID,
        buffer: &HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        if let Some(page_rc) = buffer.get(pid) {
            table.write_page_to_disk(pid, &page_rc.rl().get_page_data(&table.schema));
            return;
        } else {
            // page not found in buffer pool, so no need to write to disk
            //
            // Q: What's the possiable scenario for this case?
            // A: 1. After we implemented cache eviction feature, the page may
            // be evicted from the buffer pool.
            //    2. The page becomes empty and is discarded from the buffer
            //       pool.
        }
    }

    /// Set the page content of "pid" to the specified "page", both in
    /// the buffer pool and on disk.
    pub fn recover_page<PAGE: BTreePage>(
        pid: &BTreePageID,
        page: PAGE,
        buffer: &mut HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        // step 1: get table
        let mut catalog = Database::mut_catalog();
        let table_rc = catalog.get_table(&pid.get_table_id()).unwrap();

        // step 2: insert the page to buffer pool
        let page_rc = Arc::new(RwLock::new(page));
        buffer.insert(pid.clone(), page_rc.clone());

        // step 3: write the page to disk without write to WAL log
        let table = table_rc.read().unwrap();
        table.write_page_to_disk(pid, &page_rc.rl().get_page_data(&table.schema));
    }

    pub(crate) fn all_keys(&self) -> Vec<Key> {
        let mut keys: Vec<Key> = vec![];

        for (k, _) in &self.root_pointer_buffer {
            keys.push(k.clone());
        }

        for (k, _) in &self.header_buffer {
            keys.push(k.clone());
        }

        for (k, _) in &self.internal_buffer {
            keys.push(k.clone());
        }

        for (k, _) in &self.leaf_buffer {
            keys.push(k.clone());
        }

        keys
    }
}
