use crate::database::*;
use crate::page::*;
use crate::page_id::*;
use crate::permissions::Permissions;
use crate::table::*;
use crate::transaction_id::TransactionID;
use log::{debug, error, info};
use std::io::Read;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct BufferPool {}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {}
    }

    pub fn get_page_size() -> usize {
        4096
    }

    pub fn get_page(
        &self,
        tid: &TransactionID,
        page_id: HeapPageID,
        permission: Permissions,
    ) -> Rc<HeapPage> {
        // require lock

        // get page form buffer

        // if page not exist in buffer, get it from disk
        // let table: Arc<HeapTable> = db.get_catalog().get_table(page_id.table_id);
        let catlog = Database::global().get_catalog();
        let mut table = catlog.get_table(page_id.table_id);
        debug!("table: {:?}, table file: {:?}", table, table.get_file());

        // read page content
        let mut buffer: [u8; 4096] = [0; 4096];
        let bytes = table.get_file().read_exact(&mut buffer);
        // debug!("buffer: {:x?}", buffer);

        // convert to page object

        Rc::new(HeapPage::new(&[]))
    }
}
