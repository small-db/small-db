use std::io::SeekFrom;
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use std::{cell::RefMut, io::Seek};
use std::{collections::hash_map::Entry, io::prelude::*};

use crate::database::PAGE_SIZE;
use log::debug;

use super::file::{BTreeInternalPage, BTreePage, BTreeRootPointerPage, PageEnum};
use super::{
    database_singleton::singleton_db,
    file::{BTreeLeafPage, BTreePageID},
};

pub struct BufferPool {
    roop_pointer_buffer: HashMap<BTreePageID, Rc<Box<BTreeRootPointerPage>>>,
    internal_buffer: HashMap<BTreePageID, Rc<Box<BTreeInternalPage>>>,
    leaf_buffer: HashMap<BTreePageID, Rc<Box<BTreeLeafPage>>>,
}

type Key = BTreePageID;

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            roop_pointer_buffer: HashMap::new(),
            internal_buffer: HashMap::new(),
            leaf_buffer: HashMap::new(),
        }
    }

    pub fn get_leaf_page(&mut self, key: &Key) -> Option<Rc<Box<BTreeLeafPage>>> {
        match self.leaf_buffer.get(key) {
            Some(_) => {}
            None => {
                // get page from disk

                // 1. get db file
                let db = singleton_db();
                let pointer = db.get_catalog();
                let ct = pointer.borrow();
                let table_id = key.get_table_id();
                let f = ct.get_db_file(&table_id).unwrap();
                let btree_file = f.borrow();

                debug!("find file: {}", btree_file);
                debug!("page id: {}", key);

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
                    RefCell::new(*key),
                    buf.to_vec(),
                    btree_file.key_field,
                    btree_file.tuple_scheme.clone(),
                );

                // 4. put page into buffer pool
                self.leaf_buffer.insert(*key, Rc::new(Box::new(page)));
            }
        }

        Some(Rc::clone(self.leaf_buffer.get(key).unwrap()))
    }

    pub fn get_root_pointer_page(&mut self, key: &Key) -> Option<Rc<Box<BTreeRootPointerPage>>> {
        match self.roop_pointer_buffer.get(key) {
            Some(_) => {}
            None => {
                // get page from disk

                // 1. get db file
                let db = singleton_db();
                let pointer = db.get_catalog();
                let ct = pointer.borrow();
                let table_id = key.get_table_id();
                let f = ct.get_db_file(&table_id).unwrap();
                let btree_file = f.borrow();

                debug!("find file: {}", btree_file);
                debug!("page id: {}", key);

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
                let page = BTreeRootPointerPage::new(
                    (*key),
                    buf.to_vec(),
                );

                // 4. put page into buffer pool
                self.roop_pointer_buffer.insert(*key, Rc::new(Box::new(page)));
            }
        }

        Some(Rc::clone(self.roop_pointer_buffer.get(key).unwrap()))
    }
}
