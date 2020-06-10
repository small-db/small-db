use crate::transaction_id::TransactionID;
use crate::row::Row;
use crate::database::Database;
use crate::database::db;
use crate::permissions::Permissions;
use std::rc::Rc;

pub struct SequentialScan {
    pub tid: Rc<TransactionID>,
    pub table_id: i32,
    pub table_alias: String,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        db.get_buffer_pool().get_page(&tid, table_id, Permissions{});

        SequentialScan {
            tid: Rc::new(tid),
            table_id,
            table_alias: table_alias.to_string(),
        }
    }

//    pub fn open() {
//
//    }
//
//    pub fn next(&self) {
//    }
}

impl Iterator for SequentialScan {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
