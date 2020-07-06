// use crate::database::db;
use crate::database::Database;
use crate::page::*;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::row::Row;
use crate::row::*;
use crate::table::*;
use crate::transaction_id::TransactionID;
use log::{debug, error, info};
use std::rc::Rc;
use std::sync::{Arc, RwLockReadGuard};

pub struct SequentialScan<'a> {
    pub tid: Rc<TransactionID>,
    pub table_id: i32,
    pub table_alias: String,
    // pub page: Rc<Page>,
    pub rows: Arc<Vec<Row>>,
    index: usize,
    table: RwLockReadGuard<&'a HeapTable>,
    page_id: usize,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        debug!("start seq scan init");

        // read table's first page
        let catlog = Database::global().get_catalog();
        let table = catlog.get_table(table_id);
        let page = table.read_page(0).unwrap();
        let rows = page.get_rows();
        // debug!("rows: {:?}", rows);
        display_rows(&Arc::clone(&rows));

        debug!("finish seq scan init");

        SequentialScan {
            tid: Rc::new(tid),
            table_id,
            table_alias: table_alias.to_string(),
            rows,
            index: 0,
            // table,
            page_id: 0,
        }
    }

    pub fn rewind(&mut self) {

        let page = table.read_page(0).unwrap();
        let rows = page.get_rows();
    }
}

impl Iterator for SequentialScan {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.rows.len() {
            let row = self.rows[self.index].copy_row();
            self.index += 1;
            Some(row)
        } else {
            // read next page
            self.page_id += 1;
            self.index = 0;

            let catlog = Database::global().get_catalog();
            let table = catlog.get_table(self.table_id);
            let result = table.read_page(self.page_id);
            let page = match result {
                Ok(p) => p,
                Err(e) => {
                    debug!("error: {}", e);
                    return None;
                } ,
            };
            self.rows = page.get_rows();
            // debug!("rows: {:?}", rows);
            display_rows(&Arc::clone(&self.rows));

            self.next()
        }
    }
}
