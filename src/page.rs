use crate::row::Row;
use std::alloc::handle_alloc_error;
use std::rc::Rc;
use std::sync::Arc;

// pub trait Page {
//     // pub fn iter(&self) -> Rc<Iterator<Row>> {}
//     fn get_rows(&self) -> Arc<Vec<Row>>;
// }

pub struct HeapPage {
    rows: Arc<Vec<Row>>,
}

impl HeapPage {
    pub fn new(bytes: &[u8]) -> HeapPage {
        HeapPage {
            rows: Arc::new(Vec::new()),
        }
    }

    pub fn get_rows(&self) -> Arc<Vec<Row>> {
        Arc::clone(&self.rows)
    }
}

// impl Page for HeapPage {
//     fn get_rows(&self) -> Arc<Vec<Row>> {
//         Arc::clone(&self.rows)
//     }
// }
