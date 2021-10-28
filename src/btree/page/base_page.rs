use crate::btree::buffer_pool::BufferPool;

use super::{BTreePage, BTreePageID, PageCategory};

pub struct BTreeBasePage {
    pid: BTreePageID,
    parent_page_index: usize,
}

impl BTreeBasePage {
    pub fn new(pid: &BTreePageID) -> BTreeBasePage {
        BTreeBasePage {
            pid: pid.clone(),
            parent_page_index: 0,
        }
    }

    pub fn empty_page_data() -> Vec<u8> {
        let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
        data
    }
}

impl BTreePage for BTreeBasePage {
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
        BTreePageID::new(
            category,
            self.pid.get_table_id(),
            self.parent_page_index,
        )
    }

    fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.parent_page_index = pid.page_index;
    }
}
