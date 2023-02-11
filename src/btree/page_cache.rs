use std::{
    fs::File,
    io::{self, prelude::*, Seek, SeekFrom},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use log::{debug, error};

use super::page::{
    BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePage,
    BTreePageID, BTreeRootPointerPage, PageCategory,
};
use crate::{
    concurrent_status::Permission,
    error::SmallError,
    transaction::Transaction,
    tx_log::LogManager,
    types::{ConcurrentHashMap, ResultPod},
    utils::HandyRwLock,
    BTreeTable, Unique,
};

pub const DEFAULT_PAGE_SIZE: usize = 4096;
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_SIZE);

pub struct PageCache {
    pub root_pointer_buffer: ConcurrentHashMap<
        BTreePageID,
        Arc<RwLock<BTreeRootPointerPage>>,
    >,
    pub internal_buffer: ConcurrentHashMap<
        BTreePageID,
        Arc<RwLock<BTreeInternalPage>>,
    >,
    pub leaf_buffer:
        ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeLeafPage>>>,
    pub header_buffer:
        ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeHeaderPage>>>,
}

type Key = BTreePageID;

impl PageCache {
    pub fn new() -> Self {
        Self {
            root_pointer_buffer: ConcurrentHashMap::new(),
            header_buffer: ConcurrentHashMap::new(),
            internal_buffer: ConcurrentHashMap::new(),
            leaf_buffer: ConcurrentHashMap::new(),
        }
    }

    pub fn clear(&self) {
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
    ///
    /// reference:
    /// - https://sourcegraph.com/github.com/XiaochenCui/small-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/smalldb/BufferPool.java?L88:17&subtree=true
    fn load_page<PAGE>(&self, pid: &Key) -> ResultPod<PAGE>
    where
        PAGE: BTreePage,
    {
        // stage 1: get table
        let catalog = Unique::catalog();
        let v = catalog.get_table(&pid.get_table_id()).expect(
            &format!("table {} not found", pid.get_table_id()),
        );
        let table = v.read().unwrap();

        // stage 2: read page content from disk
        let buf = self
            .read_page(&mut table.get_file(), pid)
            .or(Err(SmallError::new("read page content failed")))?;

        // stage 3: page instantiation
        let page = PAGE::new(
            pid,
            &buf,
            &table.tuple_scheme,
            table.key_field,
        );

        // stage 4: return
        return Ok(Arc::new(RwLock::new(page)));
    }

    fn read_page(
        &self,
        file: &mut File,
        key: &Key,
    ) -> io::Result<Vec<u8>> {
        let page_size = Self::get_page_size();
        let start_pos = key.page_index as usize * page_size;
        file.seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");

        let mut buf: Vec<u8> = vec![0; page_size];
        file.read_exact(&mut buf).expect("io error");
        Ok(buf)
    }

    pub fn get_root_ptr_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeRootPointerPage> {
        Unique::concurrent_status().request_lock(
            tx,
            &perm.to_lock(),
            key,
        )?;
        self.root_pointer_buffer.get_or_insert(key, |key| {
            let page = self.load_page(key)?;
            Ok(page.clone())
        })
    }

    pub fn get_header_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeHeaderPage> {
        Unique::concurrent_status().request_lock(
            tx,
            &perm.to_lock(),
            key,
        )?;
        self.header_buffer.get_or_insert(key, |key| {
            let page = self.load_page(key)?;
            Ok(page.clone())
        })
    }

    pub fn get_internal_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeInternalPage> {
        Unique::concurrent_status().request_lock(
            tx,
            &perm.to_lock(),
            key,
        )?;
        self.internal_buffer.get_or_insert(key, |key| {
            let page = self.load_page(key)?;
            Ok(page.clone())
        })
    }

    pub fn get_leaf_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        key: &Key,
    ) -> ResultPod<BTreeLeafPage> {
        Unique::concurrent_status().request_lock(
            tx,
            &perm.to_lock(),
            key,
        )?;
        self.leaf_buffer.get_or_insert(key, |key| {
            let page = self.load_page(key)?;
            Ok(page.clone())
        })
    }

    /// Remove the specific page id from the buffer pool.
    /// Needed by the recovery manager to ensure that the
    /// buffer pool doesn't keep a rolled back page in its
    /// cache.
    ///
    /// Also used by B+ tree files to ensure that deleted pages
    /// are removed from the cache so they can be reused safely
    pub fn discard_page(&self, pid: &BTreePageID) {
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

    /// Flush all dirty pages to disk.
    ///
    /// NB: Be careful using this routine -- it writes dirty data to
    /// disk so will break small-db if running in NO STEAL mode.
    ///
    /// TODO: does these pages belong to a single table?
    pub fn flush_all_pages(&self, log_manager: &mut LogManager) {
        for pid in self.all_keys() {
            self.flush_page(&pid, log_manager);
        }
    }

    /// Write all pages of the specified transaction to disk.
    ///
    /// TODO: protest this function (mut self / or global lock)
    pub fn flush_pages(
        &self,
        tx: &Transaction,
        log_manager: &mut LogManager,
    ) {
        for pid in self.all_keys() {
            if Unique::concurrent_status().holds_lock(tx, &pid) {
                self.flush_page(&pid, log_manager);
            }
        }
    }

    pub fn tx_complete(&self, tx: &Transaction, commit: bool) {
        let mut log_manager = Unique::mut_log_manager();

        if !commit {
            for pid in self.all_keys() {
                if Unique::concurrent_status().holds_lock(tx, &pid) {
                    self.discard_page(&pid);
                }
            }

            log_manager.log_abort(tx, self).unwrap();

            return;
        }

        self.flush_pages(tx, &mut log_manager);

        for pid in self.all_keys() {
            match pid.category {
                PageCategory::Internal => {
                    self.set_before_image(
                        &pid,
                        &self.internal_buffer,
                    );
                }
                PageCategory::Leaf => {
                    self.set_before_image(&pid, &self.leaf_buffer);
                }
                PageCategory::RootPointer => {
                    self.set_before_image(
                        &pid,
                        &self.root_pointer_buffer,
                    );
                }
                PageCategory::Header => {
                    self.set_before_image(&pid, &self.header_buffer);
                }
            }
        }

        if commit {
            log_manager.log_commit(tx).unwrap();
        }
    }

    fn set_before_image<PAGE: BTreePage>(
        &self,
        pid: &BTreePageID,
        buffer: &ConcurrentHashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        let b = buffer.get_inner_wl();
        let page_pod = b.get(pid).unwrap();
        page_pod.wl().set_before_image();
    }

    /// Write the content of a specific page to disk.
    fn flush_page(
        &self,
        pid: &BTreePageID,

        log_manager: &mut LogManager,
    ) {
        // stage 1: get table
        let catalog = Unique::catalog();
        let table_pod =
            catalog.get_table(&pid.get_table_id()).unwrap();
        let table = table_pod.read().unwrap();

        match pid.category {
            PageCategory::RootPointer => {
                self.write(
                    &table,
                    pid,
                    &self.root_pointer_buffer,
                    log_manager,
                );
            }
            PageCategory::Header => {
                self.write(
                    &table,
                    pid,
                    &self.header_buffer,
                    log_manager,
                );
            }
            PageCategory::Internal => {
                self.write(
                    &table,
                    pid,
                    &self.internal_buffer,
                    log_manager,
                );
            }
            PageCategory::Leaf => {
                self.write(
                    &table,
                    pid,
                    &self.leaf_buffer,
                    log_manager,
                );
            }
        }
    }

    fn write<PAGE: BTreePage>(
        &self,
        table: &BTreeTable,
        pid: &BTreePageID,
        buffer: &ConcurrentHashMap<BTreePageID, Arc<RwLock<PAGE>>>,
        log_manager: &mut LogManager,
    ) {
        let b = buffer.get_inner_wl();
        let page_pod = b.get(pid).unwrap().clone();

        // TODO: what's the purpose of this block?
        {
            // TODO: get tx from somewhere
            if let Some(tx) =
                Unique::concurrent_status().get_page_tx(pid)
            {
                log_manager
                    .log_update(&tx, page_pod.clone())
                    .unwrap();
            } else {
                // error!("no tx found for page {:?}", pid);
                // panic!();
            }
        }

        debug!("flushing page {:?}", pid);
        table.write_page_to_disk(pid, &page_pod.rl().get_page_data());
    }

    pub fn recover_page<PAGE: BTreePage>(
        &self,
        pid: &BTreePageID,
        page: PAGE,
        buffer: &ConcurrentHashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        // step 1: get table
        let catalog = Unique::catalog();
        let table_pod =
            catalog.get_table(&pid.get_table_id()).unwrap();
        let table = table_pod.read().unwrap();

        let page_pod = Arc::new(RwLock::new(page));

        self.insert_page_dispatch(pid, &page_pod, buffer);
        self.force_flush_dispatch(pid, &table, buffer, page_pod);
    }

    // write a page to disk without write to WAL log
    fn force_flush_dispatch<PAGE: BTreePage>(
        &self,
        pid: &BTreePageID,
        table: &BTreeTable,
        buffer: &ConcurrentHashMap<BTreePageID, Arc<RwLock<PAGE>>>,
        page_pod: Arc<RwLock<PAGE>>,
    ) {
        let b = buffer.get_inner_wl();
        let page_pod = b.get(pid).unwrap().clone();

        debug!("force flushing page {:?}", pid);
        table.write_page_to_disk(pid, &page_pod.rl().get_page_data());
    }

    fn insert_page_dispatch<PAGE: BTreePage + ?Sized>(
        &self,
        pid: &BTreePageID,
        page: &Arc<RwLock<PAGE>>,
        buffer: &ConcurrentHashMap<BTreePageID, Arc<RwLock<PAGE>>>,
    ) {
        let mut b = buffer.get_inner_wl();
        b.insert(pid.clone(), page.clone());
    }

    fn all_keys(&self) -> Vec<Key> {
        let mut keys = vec![];
        keys.append(&mut self.root_pointer_buffer.keys());
        keys.append(&mut self.header_buffer.keys());
        keys.append(&mut self.leaf_buffer.keys());
        keys.append(&mut self.internal_buffer.keys());
        keys
    }
}
