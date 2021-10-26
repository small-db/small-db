use log::info;

use crate::{util::simple_int_tuple_scheme, Tuple};

use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{prelude::*, Result, Seek, SeekFrom},
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

use std::{mem, sync::Once};

use super::{
    catalog::Catalog,
    page::{
        BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage, BTreePageID,
        BTreeRootPointerPage, PageCategory,
    },
    tuple::TupleScheme,
};

pub const DEFAULT_PAGE_SIZE: usize = 4096;
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_PAGE_SIZE);

pub struct BufferPool {
    roop_pointer_buffer:
        HashMap<BTreePageID, Rc<RefCell<BTreeRootPointerPage>>>,
    pub internal_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeInternalPage>>>,
    pub leaf_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeLeafPage>>>,
    pub header_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeHeaderPage>>>,
}

type Key = BTreePageID;

impl BufferPool {
    fn new() -> BufferPool {
        BufferPool {
            roop_pointer_buffer: HashMap::new(),
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
            // SINGLETON.as_ref().unwrap()
            SINGLETON.as_mut().unwrap()
        }
    }

    pub fn clear(&mut self) {
        self.roop_pointer_buffer.clear();
        self.internal_buffer.clear();
        self.leaf_buffer.clear();
    }

    fn read_page(&self, file: &mut File, key: &Key) -> Result<Vec<u8>> {
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
    ) -> Result<Rc<RefCell<BTreeInternalPage>>> {
        match self.internal_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.borrow();

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
                self.internal_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.internal_buffer.get(key).unwrap()))
    }

    pub fn get_leaf_page(
        &mut self,
        key: &Key,
    ) -> Result<Rc<RefCell<BTreeLeafPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.borrow();

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
                self.leaf_buffer.insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.leaf_buffer.get(key).unwrap()))
    }

    pub fn get_header_page(
        &mut self,
        key: &Key,
    ) -> Result<Rc<RefCell<BTreeHeaderPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.borrow();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeHeaderPage::new(key);

                // 4. put page into buffer pool
                self.header_buffer.insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.header_buffer.get(key).unwrap()))
    }

    pub fn get_root_pointer_page(
        &mut self,
        key: &Key,
    ) -> Result<Rc<RefCell<BTreeRootPointerPage>>> {
        match self.roop_pointer_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v =
                    Catalog::global().get_table(&key.get_table_id()).unwrap();
                let table = v.borrow();

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
                self.roop_pointer_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.roop_pointer_buffer.get(key).unwrap()))
    }

    /**
    Remove the specific page id from the buffer pool.
    Needed by the recovery manager to ensure that the
    buffer pool doesn't keep a rolled back page in its
    cache.

    Also used by B+ tree files to ensure that deleted pages
    are removed from the cache so they can be reused safely
    */
    pub fn discard_page(&mut self, pid: &BTreePageID) {
        match pid.category {
            PageCategory::Internal => {
                self.internal_buffer.remove(pid);
            }
            PageCategory::Leaf => {
                self.leaf_buffer.remove(pid);
            }
            PageCategory::RootPointer => {
                self.roop_pointer_buffer.remove(pid);
            }
            PageCategory::Header => todo!(),
        }
    }

    pub fn set_page_size(page_size: usize) {
        PAGE_SIZE.store(page_size, Ordering::Relaxed);

        info!("set page size to {}", page_size);
        let scheme = simple_int_tuple_scheme(2, "");
        info!(
            "leaf page slot count: {}",
            BTreeLeafPage::calculate_slots_count(&scheme)
        );
        info!(
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

    /**
    Add a tuple to the specified table on behalf of transaction tid.  Will
    acquire a write lock on the page the tuple is added to and any other
    pages that are updated (Lock acquisition is not needed for lab2).
    May block if the lock(s) cannot be acquired.

    Marks any pages that were dirtied by the operation as dirty by calling
    their markDirty bit, and adds versions of any pages that have
    been dirtied to the cache (replacing any existing versions of those pages) so
    that future requests see up-to-date pages.
    */
    pub fn insert_tuple(&mut self, table_id: i32, t: Tuple) {
        let v = Catalog::global().get_table(&table_id).unwrap().borrow();
        v.insert_tuple(&t);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        btree::page::PageCategory, util::simple_int_tuple_scheme, BTreeTable,
    };

    use super::*;

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
        Catalog::global().add_table(Rc::new(RefCell::new(table)));

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
