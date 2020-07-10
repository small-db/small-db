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
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}
