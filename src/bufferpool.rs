use crate::transaction_id::TransactionID;
use crate::permissions::Permissions;
use crate::page::*;
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

    pub fn get_page(tid: TransactionID, table_id: i32, permission: Permissions) -> Rc<dyn Page> {
        Rc::new(HeapPage{})
    }
}

