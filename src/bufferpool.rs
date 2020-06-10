use crate::transaction_id::TransactionID;
use crate::permissions::Permissions;
use crate::page::*;
use crate::page_id::*;
use crate::database::*;
use std::rc::Rc;

pub struct BufferPool {
}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool{}
    }

    pub fn get_page_size() -> usize {
        4096
    }

    pub fn get_page(&self, tid: &TransactionID, page_id: impl PageID, permission: Permissions) -> Rc<dyn Page> {
//        require lock

//        get page form buffer

//        if page not exist in buffer, get it from disk
        let table = db.get_catalog().get_table(page_id.get_table_id());

        Rc::new(HeapPage{})
    }
}

