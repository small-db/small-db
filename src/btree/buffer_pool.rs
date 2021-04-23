use std::{io::SeekFrom};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use std::{io::Seek};
use std::{io::prelude::*};

use log::debug;
use std::mem;
use std::sync::Once;



use crate::util::simple_int_tuple_scheme;

use super::file::{
    BTreeFile, BTreeInternalPage, BTreePage, BTreeRootPointerPage, PageCategory, PageEnum,
};
use super::{
    // database_singleton::singleton_db,
    catalog::Catalog,
    file::{BTreeLeafPage, BTreePageID},
};

pub const PAGE_SIZE: usize = 4096;

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

    pub fn get_internal_page(&mut self, key: &Key) -> Option<Rc<RefCell<BTreeInternalPage>>> {
        match self.internal_buffer.get(key) {
            Some(_) => {}
            None => {
                // get page from disk
                debug!("get page from disk, pid: {}", key);

                // 1. get db file
                let v = Catalog::global().get_db_file(key.get_table_id()).unwrap();
                let btree_file = v.borrow();

                // 2. read page content
                let start_pos = BTreeRootPointerPage::page_size() + key.page_index * PAGE_SIZE;

                match btree_file
                    .get_file()
                    .seek(SeekFrom::Start(start_pos as u64))
                {
                    Ok(_) => (),
                    Err(_) => return None,
                }

                let mut buf: [u8; 4096] = [0; 4096];
                btree_file.get_file().read_exact(&mut buf);

                // 3. instantiate page
                let page =
                    BTreeInternalPage::new(RefCell::new(*key), buf.to_vec(), btree_file.key_field);

                // 4. put page into buffer pool
                self.internal_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Some(Rc::clone(self.internal_buffer.get(key).unwrap()))
    }

    pub fn get_leaf_page(&mut self, key: &Key) -> Option<Rc<RefCell<BTreeLeafPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // get page from disk
                debug!("get page from disk, pid: {}", key);

                // 1. get db file
                let v = Catalog::global().get_db_file(key.get_table_id()).unwrap();
                let btree_file = v.borrow();

                // 2. read page content
                let start_pos = BTreeRootPointerPage::page_size() + key.page_index * PAGE_SIZE;

                match btree_file
                    .get_file()
                    .seek(SeekFrom::Start(start_pos as u64))
                {
                    Ok(_) => (),
                    Err(_) => return None,
                }

                let mut buf: [u8; 4096] = [0; 4096];
                btree_file.get_file().read_exact(&mut buf);

                // 3. instantiate page
                let page = BTreeLeafPage::new(
                    key,
                    buf.to_vec(),
                    btree_file.key_field,
                    btree_file.tuple_scheme.clone(),
                );

                // 4. put page into buffer pool
                self.leaf_buffer.insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Some(Rc::clone(self.leaf_buffer.get(key).unwrap()))
    }

    pub fn get_root_pointer_page(
        &mut self,
        key: &Key,
    ) -> Option<Rc<RefCell<BTreeRootPointerPage>>> {
        match self.roop_pointer_buffer.get(key) {
            Some(_) => {}
            None => {
                // get page from disk
                debug!("get page from disk, pid: {}", key);

                // 1. get db file
                let v = Catalog::global().get_db_file(key.get_table_id()).unwrap();
                let db_file = v.borrow();

                // 2. read page content
                let start_pos = BTreeRootPointerPage::page_size() + key.page_index * PAGE_SIZE;

                match db_file.get_file().seek(SeekFrom::Start(start_pos as u64)) {
                    Ok(_) => (),
                    Err(_) => return None,
                }

                let mut buf: [u8; 4096] = [0; 4096];
                db_file.get_file().read_exact(&mut buf);

                // 3. instantiate page
                let page = BTreeRootPointerPage::new(*key, buf.to_vec());

                // 4. put page into buffer pool
                self.roop_pointer_buffer
                    .insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        Some(Rc::clone(self.roop_pointer_buffer.get(key).unwrap()))
    }
}

#[test]
fn test_buffer_pool() {
    let bp = BufferPool::global();

    // add table to catalog
    let table = BTreeFile::new("test_buffer_pool.db", 0, simple_int_tuple_scheme(3, ""));
    let table_id = table.get_id();
    Catalog::global().add_table(Rc::new(RefCell::new(table)));

    // write page to disk

    // get page
    let page_id = BTreePageID::new(PageCategory::ROOT_POINTER, table_id, 0);
    bp.get_root_pointer_page(&page_id);

    let page_id = BTreePageID::new(PageCategory::LEAF, table_id, 1);
    bp.get_root_pointer_page(&page_id);

    let page_id = BTreePageID::new(PageCategory::LEAF, table_id, 1);
    bp.get_root_pointer_page(&page_id);
}
