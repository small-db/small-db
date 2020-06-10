use crate::transaction_id::TransactionID;
use crate::permissions::Permissions;
use crate::page::*;
use crate::page_id::*;
use crate::database::*;
use log::{debug, error, info};
use std::rc::Rc;
use crate::table::{HeapTable, Table};
use std::sync::Arc;

pub struct BufferPool {
}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool{}
    }

    pub fn get_page_size() -> usize {
        4096
    }

    pub fn get_page(&self, tid: &TransactionID, page_id: HeapPageID, permission: Permissions) -> Rc<dyn Page> {
//        require lock

//        get page form buffer

//        if page not exist in buffer, get it from disk
        let table: Arc<dyn Table> = db.get_catalog().get_table(page_id.table_id);
        debug!("table: {:?}, table file: {:?}", table, table.get_file());

        Rc::new(HeapPage{})
    }
}

