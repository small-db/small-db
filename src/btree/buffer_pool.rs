use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, prelude::*, Seek, SeekFrom},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use log::{debug, error};

use super::page::{
    BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePage, BTreePageID,
    BTreeRootPointerPage, PageCategory,
};
use crate::{
    concurrent_status::Permission,
    error::SmallError,
    transaction::{LogManager, Transaction},
    types::ResultPod,
    utils::HandyRwLock,
    BTreeTable, Database,
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
        // operation, which requires the permission of the page.
        //
        // 2. If we request the lock on a page after get the access to
        // buffer pool, the request may be blocked by other
        // transactions. But we have already hold the access
        // to the buffer pool, which leads to deadlock.
        //    e.g:
        //    T1: hold page1, request buffer pool (for other pages)
        //    T2: hold buffer pool, request page1
        //    => deadlock
        //
        // 3. The lock scope of buffer pool should be as small as
        // possible, since most of its operations require
        // exclusive access.

        // // step 1: request lock from concurrent status
        // //
        // // Only acquire lock for leaf pages
        // if key.category == PageCategory::Leaf {
        //     Database::concurrent_status().request_lock(tx, &perm.to_lock(), key)?;
        // }
        if perm == Permission::ReadWrite {
            Database::concurrent_status().add_relation(tx, key);
        }

        // step 2: get page from buffer pool
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

    pub fn get_header_page(
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
    pub fn discard_page(&mut self, pid: &BTreePageID) {
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

    pub fn tx_complete(&mut self, tx: &Transaction, commit: bool) {
        let mut log_manager = Database::mut_log_manager();

        if !commit {
            for pid in self.all_keys() {
                if Database::concurrent_status().holds_lock(tx, &pid) {
                    self.discard_page(&pid);
                }
            }
            return;
        }

        // TODO: Why we need to flush all ralated pages here?
        for pid in Database::concurrent_status().get_dirty_pages(tx) {
            self.flush_page(&pid, &mut log_manager);

            match pid.category {
                PageCategory::Internal => {
                    self.set_before_image(&pid, &self.internal_buffer);
                }
                PageCategory::Leaf => {
                    self.set_before_image(&pid, &self.leaf_buffer);
                }
                PageCategory::RootPointer => {
                    self.set_before_image(&pid, &self.root_pointer_buffer);
                }
                PageCategory::Header => {
                    self.set_before_image(&pid, &self.header_buffer);
                }
            }
        }
    }

    fn set_before_image<PAGE: BTreePage>(
        &self,
        pid: &BTreePageID,
        buffer: &HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        let page_pod = buffer.get(pid).unwrap();
        page_pod.wl().set_before_image();
    }

    /// Flush all dirty pages to disk.
    ///
    /// NB: Be careful using this routine -- it writes dirty data to
    /// disk so will break small-db if running in NO STEAL mode.
    pub fn flush_all_pages(&self, log_manager: &mut LogManager) {
        for pid in self.all_keys() {
            self.flush_page(&pid, log_manager);
        }
    }

    /// Write all pages of the specified transaction to disk.
    pub fn flush_pages(&self, tx: &Transaction, log_manager: &mut LogManager) {
        for pid in Database::concurrent_status().get_dirty_pages(tx) {
            self.flush_page(&pid, log_manager);
        }
    }

    /// Write the content of a specific page to disk.
    fn flush_page(&self, pid: &BTreePageID, log_manager: &mut LogManager) {
        // stage 1: get table
        let mut catalog = Database::mut_catalog();
        let table_pod = catalog.get_table(&pid.get_table_id()).unwrap();
        let table = table_pod.read().unwrap();

        match pid.category {
            PageCategory::RootPointer => {
                if !self.root_pointer_buffer.contains_key(pid) {
                    // page not found in buffer pool, so no need to write to disk
                    //
                    // why there are some pages not in buffer pool?
                    return;
                }

                self.write(&table, pid, &self.root_pointer_buffer, log_manager);
                self.set_before_image(&pid, &self.root_pointer_buffer);
            }
            PageCategory::Header => {
                if !self.header_buffer.contains_key(pid) {
                    // page not found in buffer pool, so no need to write to disk
                    //
                    // why there are some pages not in buffer pool?
                    return;
                }

                self.write(&table, pid, &self.header_buffer, log_manager);
                self.set_before_image(&pid, &self.header_buffer);
            }
            PageCategory::Internal => {
                if !self.internal_buffer.contains_key(pid) {
                    // page not found in buffer pool, so no need to write to disk
                    //
                    // why there are some pages not in buffer pool?
                    return;
                }

                self.write(&table, pid, &self.internal_buffer, log_manager);
                self.set_before_image(&pid, &self.internal_buffer);
            }
            PageCategory::Leaf => {
                if !self.leaf_buffer.contains_key(pid) {
                    // page not found in buffer pool, so no need to write to disk
                    //
                    // why there are some pages not in buffer pool?
                    return;
                }

                self.write(&table, pid, &self.leaf_buffer, log_manager);
                self.set_before_image(&pid, &self.leaf_buffer);
            }
        }
    }

    fn write<PAGE: BTreePage>(
        &self,
        table: &BTreeTable,
        pid: &BTreePageID,
        buffer: &HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
        log_manager: &mut LogManager,
    ) {
        let page_pod = buffer.get(pid).unwrap().clone();

        if let Some(tx) = Database::concurrent_status().get_page_tx2(pid) {
            log_manager.log_update(&tx, page_pod.clone()).unwrap();
            table.write_page_to_disk(pid, &page_pod.rl().get_page_data());
            return;
        }

        // not a dirty page, so no need to write to log or disk, just return
        return;
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
        let table_pod = catalog.get_table(&pid.get_table_id()).unwrap();
        let table = table_pod.read().unwrap();

        let page_pod = Arc::new(RwLock::new(page));

        Self::insert_page_dispatch(pid, &page_pod, buffer);
        Self::force_flush_dispatch(pid, &table, page_pod);
    }

    // write a page to disk without write to WAL log
    fn force_flush_dispatch<PAGE: BTreePage>(
        pid: &BTreePageID,
        table: &BTreeTable,
        page_pod: Arc<RwLock<PAGE>>,
    ) {
        table.write_page_to_disk(pid, &page_pod.rl().get_page_data());
    }

    fn insert_page_dispatch<PAGE: BTreePage + ?Sized>(
        pid: &BTreePageID,
        page: &Arc<RwLock<PAGE>>,
        buffer: &mut HashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        // let mut b = buffer.get_inner_wl();
        buffer.insert(pid.clone(), page.clone());
    }

    fn all_keys(&self) -> Vec<Key> {
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
