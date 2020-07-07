use crate::database::*;
use crate::page::*;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::table::*;
use crate::transaction_id::TransactionID;
use log::{debug, error, info};
use std::io::Read;
use std::rc::Rc;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, RwLockWriteGuard},
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
        &self,
        tid: &TransactionID,
        // table_id: i32,
        // page_id: i32,
        page_id: HeapPageID,
        permission: Permissions,
    ) -> RwLockWriteGuard<HeapPage> {
        // require lock

        // get page form buffer
        let result = self.buffer.get(&page_id);
        match result {
            Some(v) => {
                // Rc::new(Arc::clone(v).into_inner().unwrap())
                v.try_write().unwrap()
            }
            None => {
                // if page not exist in buffer, get it from disk
                let catlog = Database::global().get_catalog();
                let mut table = catlog.get_table(page_id.table_id);
                let page = table.read_page(0).unwrap();
                return RwLock::new(page).try_write().unwrap()
                // return Rc::new(page);
            }
        }

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
        //     bytes.push(*b);
        // }
        // // debug!("buffer: {:x?}", buffer);

        // // convert to page object

        // Rc::new(HeapPage::new(table.get_row_scheme(), bytes))
    }
}
