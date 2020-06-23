use crate::row::*;
use crate::{page_id::HeapPageID, database::*};
use std::alloc::handle_alloc_error;
use std::rc::Rc;
use std::sync::Arc;
use log::debug;

// pub trait Page {
//     // pub fn iter(&self) -> Rc<Iterator<Row>> {}
//     fn get_rows(&self) -> Arc<Vec<Row>>;
// }

pub struct HeapPage {
    page_id: HeapPageID,
    row_scheme: Arc<RowScheme>,
    rows: Arc<Vec<Row>>,
    header: Vec<u8>,
}

impl HeapPage {
    pub fn new(page_id: HeapPageID, bytes: Vec<u8>) -> HeapPage {
        let table_id = page_id.table_id;
        let row_scheme = Database::global().get_catalog().get_row_scheme(table_id);
        let mut header: Vec<u8> = Vec::new();
        // for b in bytes[0..HeapPage::get_header_size(&row_scheme)].into_iter() {
        //     header.push(*b);
        // }
        header.append(bytes[0..HeapPage::get_header_size(&row_scheme)]);
        debug!("header: {:?}", header);
        HeapPage {
            page_id,
            row_scheme: row_scheme,
            header,
            rows: Arc::new(Vec::new()),
        }
    }

    pub fn get_rows(&self) -> Arc<Vec<Row>> {
        Arc::clone(&self.rows)
    }

    fn get_rows_count(row_scheme: &RowScheme) -> usize {
        Database::global().get_buffer_pool().get_page_size() * 8 / (row_scheme.get_size() * 8 + 1)
    }

    fn get_header_size(row_scheme: &RowScheme) -> usize {
        (HeapPage::get_rows_count(&row_scheme) + 7) / 8
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