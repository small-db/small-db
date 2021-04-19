use std::io::Seek;
use std::io::SeekFrom;
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use std::{collections::hash_map::Entry, io::prelude::*};

use crate::database::PAGE_SIZE;
use log::debug;

use super::file::BTreeRootPointerPage;
use super::{
    database_singleton::singleton_db,
    file::{BTreeLeafPage, BTreePageID},
};

// pub const BUFFER_POOL: HashMap<i32, BTreeLeafPage> = HashMap::new();

pub struct BufferPool {
    buffer: HashMap<Key, Value>,
}

type Key = BTreePageID;
type Value = Rc<RefCell<BTreeLeafPage>>;

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            buffer: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, key: &Key) -> Option<&Value> {
        match self.buffer.get(key) {
            // Entry::Occupied(_) => {}
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
                    buf.to_vec(),
                    btree_file.key_field,
                    btree_file.tuple_scheme.clone(),
                );

                // 4. put page into buffer pool
                self.buffer.insert(*key, Rc::new(RefCell::new(page)));
            }
        }

        self.buffer.get(key)
    }
}
