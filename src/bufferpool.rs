use crate::database::*;
use crate::page::*;
use crate::page_id::*;
use crate::permissions::Permissions;

use crate::transaction_id::TransactionID;
use log::debug;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

pub struct BufferPool {
    buffer: HashMap<HeapPageID, Arc<RwLock<HeapPage>>>,
}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            buffer: HashMap::new(),
        }
    }

    pub fn get_page_size(&self) -> usize {
        4096
    }

    pub fn get_page(
        &mut self,
        _tid: &TransactionID,
        // table_id: i32,
        // page_id: i32,
        page_id: HeapPageID,
        _permission: Permissions,
    ) -> Option<RwLockWriteGuard<HeapPage>> {
        // require lock

        // get page form buffer
        debug!("get page: {:?}", page_id);
        debug!("buffer: {:?}", self.buffer.keys());
        if self.buffer.contains_key(&page_id) {
            return match self.buffer.get(&page_id) {
                Some(v) => Some(v.try_write().unwrap()),
                None => unreachable!(),
            };
        }

        // if page not exist in buffer, get it from disk
        let catlog = Database::global().get_catalog();
        let mut table = catlog.get_table(page_id.table_id);
        let result = table.read_page(page_id.page_index);
        let page = match result {
            Ok(p) => p,
            Err(e) => {
                debug!("error: {}", e);
                return None;
            }
        };

        // add to buffer
        self.buffer.insert(page_id, Arc::new(RwLock::new(page)));

        return Some(self.buffer.get(&page_id).unwrap().try_write().unwrap());

        // match self.buffer.get(&page_id) {
        // Some(v) => {
        // // Rc::new(Arc::clone(v).into_inner().unwrap())
        // return v.try_write().unwrap();
        // }
        // None => {
        // return Rc::new(page);
        // }
        // }

        // // if page not exist in buffer, get it from disk
        // // let table: Arc<HeapTable> = db.get_catalog().get_table(page_id.table_id);
        // let catlog = Database::global().get_catalog();
        // let mut table = catlog.get_table(page_id.table_id);
        // debug!("table: {:?}, table file: {:?}", table, table.get_file());

        // // read page content
        // let mut buffer: [u8; 4096] = [0; 4096];
        // let mut bytes: Vec<u8> = Vec::new();
        // table.get_file().read_exact(&mut buffer);
        // for b in buffer.into_iter() {
        // bytes.push(*b);
        // }
        // // debug!("buffer: {:x?}", buffer);

        // // convert to page object

        // Rc::new(HeapPage::new(table.get_row_scheme(), bytes))
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}
