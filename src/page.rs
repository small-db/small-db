use crate::row::*;
use crate::{page_id::HeapPageID, database::*};
use std::alloc::handle_alloc_error;
use std::rc::Rc;
use std::sync::Arc;

// pub trait Page {
//     // pub fn iter(&self) -> Rc<Iterator<Row>> {}
//     fn get_rows(&self) -> Arc<Vec<Row>>;
// }

pub struct HeapPage {
    page_id: HeapPageID,
    row_scheme: Arc<RowScheme>,
    rows: Arc<Vec<Row>>,
}

impl HeapPage {
    pub fn new(page_id: HeapPageID, bytes: &[u8]) -> HeapPage {
        let table_id = page_id.table_id;
        HeapPage {
            page_id,
            row_scheme: Database::global().get_catalog().get_row_scheme(table_id),
            rows: Arc::new(Vec::new()),
        }
    }

    pub fn get_rows(&self) -> Arc<Vec<Row>> {
        Arc::clone(&self.rows)
    }

    fn get_rows_count(&self) -> usize {
        Database::global().get_buffer_pool().get_page_size() * 8 / self.row_scheme.get_size()
    }
}

// impl Page for HeapPage {
//     fn get_rows(&self) -> Arc<Vec<Row>> {
//         Arc::clone(&self.rows)
//     }
// }

// pub struct HeapPageID {
//     table_id: i32,
//     page_index: i32,
// }