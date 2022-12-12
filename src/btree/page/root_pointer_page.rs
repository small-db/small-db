use std::convert::TryInto;

use super::{
    BTreeBasePage, BTreePage, BTreePageID, PageCategory, EMPTY_PAGE_ID,
};
use crate::btree::{buffer_pool::BufferPool, tuple::TupleScheme};

/// # Binary Layout
///
/// - [0-4) (4 bytes): root page index
/// - [4-8) (4 bytes): root page category (leaf/internal)
/// - [8-12) (4 bytes): header page index
pub struct BTreeRootPointerPage {
    base: BTreeBasePage,

    /// The type of this field is `BTreePageID` instead of
    /// `Option<BTreePageID>` because the root page is always
    /// present in the B+ tree.
    ///
    /// This decision also simplified the code.
    root_pid: BTreePageID,

    /// TODO: mandatory the presence of a header page?
    header_page_index: u32,
}

impl BTreeRootPointerPage {
    fn new(pid: &BTreePageID, bytes: Vec<u8>) -> Self {
        let root_page_index =
            u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let root_page_category = PageCategory::from_bytes(&bytes[4..8]);
        let header_page_index =
            u32::from_le_bytes(bytes[8..12].try_into().unwrap());

        let root_pid = BTreePageID {
            category: root_page_category,
            page_index: root_page_index,
            table_id: pid.get_table_id(),
        };
        Self {
            base: BTreeBasePage::new(pid),
            root_pid,
            header_page_index,
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

    fn get_page_data(&self) -> Vec<u8> {
        let mut data = vec![0; BufferPool::get_page_size()];

        // Write the root page index.
        data[0..4].copy_from_slice(&self.root_pid.page_index.to_le_bytes());

        // Write the root page category.
        data[4..8].copy_from_slice(&self.root_pid.category.to_bytes());

        // Write the header page index.
        data[8..12].copy_from_slice(&self.header_page_index.to_le_bytes());

        return data;
    }
}
