// use crate::database::db;
use crate::database::Database;
use crate::page::*;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::row::Row;
use crate::transaction_id::TransactionID;
use log::{debug, error, info};
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
        debug!("start seq scan init");

        // read table's first page
        let catlog = Database::global().get_catalog();
        let mut table = catlog.get_table(table_id);
        let page = table.read_page(0);
        let rows = page.get_rows();
        debug!("rows: {:?}", rows);

        debug!("finish seq scan init");

        SequentialScan {
            tid: Rc::new(tid),
            table_id,
            table_alias: table_alias.to_string(),
            rows,
        }
    }
}

impl Iterator for SequentialScan {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        // Some(self.rows[0].copy_row())
        None
    }
}
