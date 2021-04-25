use super::consts::PAGE_SIZE;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{prelude::*, Result, Seek, SeekFrom},
    rc::Rc,
};

use log::debug;
use std::{mem, sync::Once};

use super::page::{BTreeInternalPage, BTreeRootPointerPage, PageCategory};

use super::{
    catalog::Catalog,
    page::{BTreeLeafPage, BTreePageID},
};

pub struct BufferPool {
    roop_pointer_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeRootPointerPage>>>,
    internal_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeInternalPage>>>,
    leaf_buffer: HashMap<BTreePageID, Rc<RefCell<BTreeLeafPage>>>,
}

type Key = BTreePageID;

impl BufferPool {
    fn new() -> BufferPool {
        BufferPool {
            roop_pointer_buffer: HashMap::new(),
            internal_buffer: HashMap::new(),
            leaf_buffer: HashMap::new(),
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
            // Now we give out a copy of the data that is safe to use concurrently.
            // (*SINGLETON).clone()
            // SINGLETON.as_ref().unwrap()
            SINGLETON.as_mut().unwrap()
        }
    }

    fn read_page(&self, file: &mut File, key: &Key) -> Result<Vec<u8>> {
        debug!("get page from disk, pid: {}", key);
        let start_pos: usize = match key.category {
            PageCategory::RootPointer => 0,
            _ => BTreeRootPointerPage::page_size() + (key.page_index - 1) * PAGE_SIZE,
        };
        file.seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");

        let mut buf: [u8; 4096] = [0; 4096];
        file.read_exact(&mut buf).expect("io error");
        Ok(buf.to_vec())
    }

    pub fn get_internal_page(&mut self, key: &Key) -> Result<Rc<RefCell<BTreeInternalPage>>> {
        match self.internal_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v = Catalog::global().get_table(key.get_table_id()).unwrap();
                let table = v.borrow();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeInternalPage::new(
                    RefCell::new(*key),
                    buf.to_vec(),
                    &table.tuple_scheme.fields[table.key_field],
                );

                // 4. put page into buffer pool
                self.internal_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.internal_buffer.get(key).unwrap()))
    }

    pub fn get_leaf_page(&mut self, key: &Key) -> Result<Rc<RefCell<BTreeLeafPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v = Catalog::global().get_table(key.get_table_id()).unwrap();
                let table = v.borrow();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeLeafPage::new(key, buf.to_vec(), table.tuple_scheme.clone());

                // 4. put page into buffer pool
                self.leaf_buffer.insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.leaf_buffer.get(key).unwrap()))
    }

    pub fn get_root_pointer_page(
        &mut self,
        key: &Key,
    ) -> Result<Rc<RefCell<BTreeRootPointerPage>>> {
        match self.roop_pointer_buffer.get(key) {
            Some(_) => {}
            None => {
                // 1. get table
                let v = Catalog::global().get_table(key.get_table_id()).unwrap();
                let table = v.borrow();

                // 2. read page content
                let buf = self.read_page(&mut table.get_file(), key)?;

                // 3. instantiate page
                let page = BTreeRootPointerPage::new(buf.to_vec());

                // 4. put page into buffer pool
                self.roop_pointer_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Ok(Rc::clone(self.roop_pointer_buffer.get(key).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{btree::page::PageCategory, util::simple_int_tuple_scheme, BTreeTable};

    use super::*;

    #[test]
    fn test_buffer_pool() {
        let bp = BufferPool::global();

        // add table to catalog
        let table = BTreeTable::new("test_buffer_pool.db", 0, simple_int_tuple_scheme(3, ""));
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
