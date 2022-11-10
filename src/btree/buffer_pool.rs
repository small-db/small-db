use std::{
    fs::File,
    io::{self, prelude::*, Seek, SeekFrom},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use log::debug;

use super::{
    page::{
        BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePage,
        BTreePageID, BTreeRootPointerPage, PageCategory,
    },
    tuple::TupleScheme,
};
use crate::{
    concurrent_status::{ConcurrentStatus, Permission},
    error::SmallError,
    transaction::Transaction,
    types::{ConcurrentHashMap, ResultPod},
    utils::small_int_tuple_scheme,
    Unique,
};

pub const DEFAULT_PAGE_SIZE: usize = 4096;
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_SIZE);

pub struct BufferPool {
    root_pointer_buffer:
        ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeRootPointerPage>>>,
    pub internal_buffer:
        ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeInternalPage>>>,
    pub leaf_buffer: ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeLeafPage>>>,
    pub header_buffer:
        ConcurrentHashMap<BTreePageID, Arc<RwLock<BTreeHeaderPage>>>,
}

type Key = BTreePageID;

impl BufferPool {
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
    /// Will acquire a lock and may block if that lock is held by another
    /// transaction.
    ///
    /// The retrieved page should be looked up in the buffer pool.  If it
    /// is present, it should be returned.  If it is not present, it should
    /// be added to the buffer pool and returned.  If there is insufficient
    /// space in the buffer pool, a page should be evicted and the new page
    /// should be added in its place.
    ///
    /// reference:
    /// - https://sourcegraph.com/github.com/XiaochenCui/small-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/smalldb/BufferPool.java?L88:17&subtree=true
    fn load_page<PAGE: BTreePage>(&self, key: &Key) -> ResultPod<PAGE> {
        // stage 1: get table
        let catalog = Unique::catalog();
        let v = catalog.get_table(&key.get_table_id()).unwrap();
        let table = v.read().unwrap();

        // stage 2: read page content from disk
        let buf = self
            .read_page(&mut table.get_file(), key)
            .or(Err(SmallError::new("read page content failed")))?;

        // stage 3: page instantiation
        let page =
            PAGE::new(key, buf.to_vec(), &table.tuple_scheme, table.key_field);

        // stage 4: return
        return Ok(Arc::new(RwLock::new(page)));
    }

    fn read_page(&self, file: &mut File, key: &Key) -> io::Result<Vec<u8>> {
        let page_size = Self::get_page_size();
        let start_pos = key.page_index * page_size;
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
        Unique::concurrent_status().request_lock(tx, &perm.to_lock(), key)?;
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
        Unique::concurrent_status().request_lock(tx, &perm.to_lock(), key)?;
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
        Unique::concurrent_status().request_lock(tx, &perm.to_lock(), key)?;
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
        Unique::concurrent_status().request_lock(tx, &perm.to_lock(), key)?;
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

        debug!("set page size to {}", page_size);
        let scheme = small_int_tuple_scheme(2, "");
        debug!(
            "leaf page slot count: {}",
            BTreeLeafPage::calculate_slots_count(&scheme)
        );
        debug!(
            "internal page entries count: {}, children count: {}",
            BTreeInternalPage::get_max_entries(4),
            BTreeInternalPage::get_max_entries(4) + 1,
        );
    }

    pub fn rows_per_page(scheme: &TupleScheme) -> usize {
        BTreeLeafPage::calculate_slots_count(&scheme)
    }

    pub fn children_per_page() -> usize {
        BTreeInternalPage::get_max_entries(4) + 1
    }

    pub fn get_page_size() -> usize {
        PAGE_SIZE.load(Ordering::Relaxed)
    }

    // /// Add a tuple to the specified table on behalf of transaction tid.
    // Will /// acquire a write lock on the page the tuple is added to and
    // any other /// pages that are updated (Lock acquisition is not needed
    // for lab2). /// May block if the lock(s) cannot be acquired.
    // ///
    // /// Marks any pages that were dirtied by the operation as dirty by
    // calling /// their markDirty bit, and adds versions of any pages that
    // have /// been dirtied to the cache (replacing any existing versions
    // of those /// pages) so that future requests see up-to-date pages.
    // pub fn insert_tuple(
    //     &mut self,
    //     tx: &Transaction,
    //     table_id: i32,
    //     t: &Tuple,
    // ) -> SmallResult {
    //     let v = Unique::catalog().get_table(&table_id).unwrap().rl();
    //     v.insert_tuple(tx, t)?;
    //     return Ok(());
    // }
}
