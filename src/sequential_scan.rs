// use crate::database::db;
use crate::database::Database;

use crate::page_id::*;
use crate::permissions::Permissions;
use crate::row::Row;
use crate::row::*;

use crate::transaction_id::TransactionID;

use log::debug;
use std::rc::Rc;
use std::sync::Arc;

pub struct SequentialScan {
    pub tid: Rc<TransactionID>,
    pub table_id: i32,
    pub table_alias: String,
    // pub page: Rc<Page>,
    pub rows: Arc<Vec<Row>>,
    index: usize,
    // table: RwLockReadGuard<HeapTable>,
    page_id: usize,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        debug!("start seq scan init");

        // read table's first page
        let mut buffer_pool = Database::global().get_buffer_pool();
        let option = buffer_pool.get_page(
            &TransactionID { id: 0 },
            HeapPageID {
                table_id,
                page_index: 0,
            },
            Permissions {},
        );
        let page = match option {
            Some(p) => p,
            None => unreachable!(),
        };

        let rows = page.get_rows();
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
        // read table's first page
        let mut buffer_pool = Database::global().get_buffer_pool();
        let option = buffer_pool.get_page(
            &TransactionID { id: 0 },
            HeapPageID {
                table_id: self.table_id,
                page_index: 0,
            },
            Permissions {},
        );
        let page = match option {
            Some(p) => p,
            None => unreachable!(),
        };
        let rows = page.get_rows();
        display_rows(&Arc::clone(&rows));

        self.rows = rows;
        self.index = 0;
        self.page_id = 0;
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

            let mut buffer_pool = Database::global().get_buffer_pool();
            let option = buffer_pool.get_page(
                &TransactionID { id: 0 },
                HeapPageID {
                    table_id: self.table_id,
                    page_index: self.page_id,
                },
                Permissions {},
            );
            let page = match option {
                Some(p) => p,
                None => {
                    return None;
                }
            };
            self.rows = page.get_rows();
            display_rows(&Arc::clone(&self.rows));

            self.next()
        }
    }
}
