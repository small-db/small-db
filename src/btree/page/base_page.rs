use super::{BTreePage, BTreePageID, BTreePageInit, PageCategory};
use crate::{btree::buffer_pool::BufferPool, storage::table_schema::TableSchema};

pub struct BTreeBasePage {
    pid: BTreePageID,
    parent_page_index: u32,
}

impl BTreeBasePage {
    pub fn new(pid: &BTreePageID) -> BTreeBasePage {
        BTreeBasePage {
            pid: pid.clone(),
            parent_page_index: 0,
        }
    }
}

impl BTreePageInit for BTreeBasePage {
    fn new_empty_page(pid: &BTreePageID, table_schema: &TableSchema) -> Self {
        panic!("BTreeBasePage::new_empty_page should not be called");
    }
}

impl BTreePage for BTreeBasePage {
    fn new(pid: &BTreePageID, _bytes: &[u8], _tuple_scheme: &TableSchema) -> Self {
        Self::new(pid)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    fn get_parent_pid(&self) -> BTreePageID {
        let category: PageCategory;
        if self.parent_page_index == 0 {
            category = PageCategory::RootPointer;
        } else {
            category = PageCategory::Internal;
        }
        BTreePageID::new(category, self.pid.get_table_id(), self.parent_page_index)
    }

    fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.parent_page_index = pid.page_index;
    }

    fn get_page_data(&self, _table_schema: &TableSchema) -> Vec<u8> {
        unimplemented!()
    }

    fn set_before_image(&mut self, _table_schema: &TableSchema) {
        unimplemented!()
    }

    fn get_before_image(&self, _table_schema: &TableSchema) -> Vec<u8> {
        unimplemented!()
    }
}
