// use crate::database::db;
use crate::database::Database;
use crate::page::Page;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::row::Row;
use crate::transaction_id::TransactionID;
use std::rc::Rc;
use std::sync::Arc;

pub struct SequentialScan {
    pub tid: Rc<TransactionID>,
    pub table_id: i32,
    pub table_alias: String,
    // pub page: Rc<Page>,
    pub rows: Arc<Vec<Row>>,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        // let page_id = HeapPageID {
        // table_id: table_id,
        // page_index: 0,
        // };
        // let page = Database::global()
        // .get_buffer_pool()
        // .get_page(&tid, page_id, Permissions {});
        // let rows = page.get_rows();

        // read table's first page
        let catlog = Database::global().get_catalog();
        let mut table = catlog.get_table(table_id);
        let page = table.read_page(0);
        let rows = page.get_rows();

        SequentialScan {
            tid: Rc::new(tid),
            table_id,
            table_alias: table_alias.to_string(),
            // page,
            rows,
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
        // Some(self.rows[0])
        None
    }
}
