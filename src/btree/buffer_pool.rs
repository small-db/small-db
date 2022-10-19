use std::{
    collections::HashMap,
    fs::File,
    io::{self, prelude::*, Seek, SeekFrom},
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Once, RwLock,
    },
};

use log::debug;

use super::{
    catalog::Catalog,
    page::{
        BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePageID,
        BTreeRootPointerPage, PageCategory,
    },
    tuple::TupleScheme,
};
use crate::{
    error::MyError,
    transaction::Transaction,
    utils::{simple_int_tuple_scheme, HandyRwLock},
    Tuple,
};

pub const DEFAULT_PAGE_SIZE: usize = 4096;
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_SIZE);

pub struct BufferPool {
    root_pointer_buffer:
        HashMap<BTreePageID, Arc<RwLock<BTreeRootPointerPage>>>,
    pub internal_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeInternalPage>>>,
    pub leaf_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeLeafPage>>>,
    pub header_buffer: HashMap<BTreePageID, Arc<RwLock<BTreeHeaderPage>>>,
}

type Key = BTreePageID;

impl BufferPool {
    fn new() -> BufferPool {
        BufferPool {
            root_pointer_buffer: HashMap::new(),
            internal_buffer: HashMap::new(),
            leaf_buffer: HashMap::new(),
            header_buffer: HashMap::new(),
        }
    }

    pub fn global() -> &'static mut Self {
        // Initialize it to a null value
        static mut SINGLETON: *mut BufferPool = 0 as *mut BufferPool;
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
            // concurrently. (*SINGLETON).clone()
            SINGLETON.as_mut().unwrap()
        }
    }

    pub fn clear(&mut self) {
        self.root_pointer_buffer.clear();
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
    /// - https://sourcegraph.com/github.com/XiaochenCui/simple-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/simpledb/BufferPool.java?L88:17&subtree=true
    pub fn get_page() {}

    fn read_page(&self, file: &mut File, key: &Key) -> io::Result<Vec<u8>> {
        let page_size = Self::get_page_size();
        let start_pos = key.page_index * page_size;
        file.seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");

        let mut buf: Vec<u8> = vec![0; page_size];
        file.read_exact(&mut buf).expect("io error");
        Ok(buf)
    }

    pub fn get_internal_page(
        &mut self,
        key: &Key,
    ) -> io::Result<Arc<RwLock<BTreeInternalPage>>> {
        match self.internal_buffer.get(key) {
            Some(v) => {
                return Ok(v.clone());
            }
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.read().unwrap();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeInternalPage::new(
                    key,
                    buf.to_vec(),
                    &table.tuple_scheme,
                    table.key_field,
                );

                // 4. put page into buffer pool
                let v = Arc::new(RwLock::new(page));
                self.internal_buffer.insert(*key, v.clone());
                return Ok(v.clone());
            }
        }
    }

    pub fn get_leaf_page(
        &mut self,
        key: &Key,
    ) -> io::Result<Arc<RwLock<BTreeLeafPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.rl();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeLeafPage::new(
                    key,
                    buf.to_vec(),
                    &table.tuple_scheme,
                    table.key_field,
                );

                // 4. put page into buffer pool
                self.leaf_buffer.insert(*key, Arc::new(RwLock::new(page)));
            }
        }

        Ok(Arc::clone(self.leaf_buffer.get(key).unwrap()))
    }

    pub fn get_header_page(
        &mut self,
        key: &Key,
    ) -> io::Result<Arc<RwLock<BTreeHeaderPage>>> {
        match self.header_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.rl();

                // 2. read page content
                let _buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeHeaderPage::new(key);

                // 4. put page into buffer pool
                self.header_buffer.insert(*key, Arc::new(RwLock::new(page)));
            }
        }

        Ok(Arc::clone(self.header_buffer.get(key).unwrap()))
    }

    pub fn get_root_pointer_page(
        &mut self,
        key: &Key,
    ) -> io::Result<Arc<RwLock<BTreeRootPointerPage>>> {
        match self.root_pointer_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.rl();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let pid = BTreePageID::new(
                    PageCategory::RootPointer,
                    table.get_id(),
                    0,
                );
                let page = BTreeRootPointerPage::new(&pid, buf.to_vec());

                // 4. put page into buffer pool
                self.root_pointer_buffer
                    .insert(*key, Arc::new(RwLock::new(page)));
            }
        }

        Ok(Arc::clone(self.root_pointer_buffer.get(key).unwrap()))
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
            PageCategory::Header => todo!(),
        }
    }

    pub fn set_page_size(page_size: usize) {
        PAGE_SIZE.store(page_size, Ordering::Relaxed);

        debug!("set page size to {}", page_size);
        let scheme = simple_int_tuple_scheme(2, "");
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

    /// Add a tuple to the specified table on behalf of transaction tid.  Will
    /// acquire a write lock on the page the tuple is added to and any other
    /// pages that are updated (Lock acquisition is not needed for lab2).
    /// May block if the lock(s) cannot be acquired.
    ///
    /// Marks any pages that were dirtied by the operation as dirty by calling
    /// their markDirty bit, and adds versions of any pages that have
    /// been dirtied to the cache (replacing any existing versions of those
    /// pages) so that future requests see up-to-date pages.
    pub fn insert_tuple(
        &mut self,
        table_id: i32,
        tx: &Transaction,
        t: &Tuple,
    ) -> Result<(), MyError> {
        let v = Catalog::global().get_table(&table_id).unwrap().rl();
        v.insert_tuple(tx, t)?;
        return Ok(());
    }

    pub fn insert_tuple_auto_tx(
        &mut self,
        table_id: i32,
        tuple: &Tuple,
    ) -> Result<(), MyError> {
        let tx = Transaction::new();
        self.insert_tuple(table_id, &tx, tuple)?;
        tx.commit();
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::{
        btree::page::PageCategory, utils::simple_int_tuple_scheme, BTreeTable,
    };

    #[test]
    fn test_buffer_pool() {
        let bp = BufferPool::global();

        // add table to catalog
        let table = BTreeTable::new(
            "test_buffer_pool.db",
            0,
            &simple_int_tuple_scheme(3, ""),
        );
        let table_id = table.get_id();
        Catalog::global().add_table(Arc::new(RwLock::new(table)));

        // write page to disk

        // get page
        let page_id = BTreePageID::new(PageCategory::RootPointer, table_id, 0);
        match bp.get_root_pointer_page(&page_id) {
            Ok(_) => {}
            Err(e) => {
                panic!("err: {}", e)
            }
        }

        let page_id = BTreePageID::new(PageCategory::Leaf, table_id, 1);
        match bp.get_root_pointer_page(&page_id) {
            Ok(_) => {}
            Err(e) => {
                panic!("err: {}", e)
            }
        }

        let page_id = BTreePageID::new(PageCategory::Leaf, table_id, 1);
        match bp.get_root_pointer_page(&page_id) {
            Ok(_) => {}
            Err(e) => {
                panic!("err: {}", e)
            }
        }
    }
}
