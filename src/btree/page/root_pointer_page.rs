use std::{any::Any, convert::TryInto};

use super::{
    BTreeBasePage, BTreePage, BTreePageID, PageCategory, EMPTY_PAGE_ID,
};
use crate::btree::tuple::TupleScheme;

pub struct BTreeRootPointerPage {
    base: BTreeBasePage,

    // The root_pid in mandatory to avoid a bunch of Option & match
    root_pid: BTreePageID,

    header_page_index: usize,
}

impl BTreeRootPointerPage {
    pub fn new(pid: &BTreePageID, bytes: Vec<u8>) -> Self {
        let root_page_index =
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: root_page_index,

            // TODO: set table id
            table_id: 0,
        };
        Self {
            base: BTreeBasePage::new(pid),
            root_pid,
            header_page_index: EMPTY_PAGE_ID,
        }
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }

    /// Get the id of the first header page
    pub fn get_header_pid(&self) -> Option<BTreePageID> {
        if self.header_page_index == EMPTY_PAGE_ID {
            None
        } else {
            Some(BTreePageID::new(
                PageCategory::Header,
                self.get_pid().table_id,
                self.header_page_index,
            ))
        }
    }

    /// Set the page id of the first header page
    pub fn set_header_pid(&mut self, pid: &BTreePageID) {
        self.header_page_index = pid.page_index;
    }
}

impl BTreePage for BTreeRootPointerPage {
    fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        _tuple_scheme: &TupleScheme,
        _key_field: usize,
    ) -> Self {
        Self::new(pid, bytes)
    }

    fn get_pid(&self) -> BTreePageID {
        self.base.get_pid()
    }

    fn get_parent_pid(&self) -> BTreePageID {
        self.base.get_parent_pid()
    }

    fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.base.set_parent_pid(pid)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
