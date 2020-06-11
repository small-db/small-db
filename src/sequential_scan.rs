// use crate::database::db;
use crate::database::Database;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::row::Row;
use crate::transaction_id::TransactionID;
use std::rc::Rc;

pub struct SequentialScan {
    pub tid: Rc<TransactionID>,
    pub table_id: i32,
    pub table_alias: String,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        let page_id = HeapPageID {
            table_id: table_id,
            page_index: 0,
        };
        Database::global()
            .get_buffer_pool()
            .get_page(&tid, page_id, Permissions {});

        SequentialScan {
            tid: Rc::new(tid),
            table_id,
            table_alias: table_alias.to_string(),
        }
    }

    // pub fn open() {
    //
    // }
    //
    // pub fn next(&self) {
    // }
}

impl Iterator for SequentialScan {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
