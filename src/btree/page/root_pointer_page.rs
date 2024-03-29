use std::io::Cursor;

use log::debug;

use super::{BTreeBasePage, BTreePage, BTreePageID, PageCategory, EMPTY_PAGE_ID};
use crate::{
    btree::buffer_pool::BufferPool,
    io::{Decodeable, SmallWriter},
    storage::schema::Schema,
};

/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: root page index
/// - 4 bytes: root page category (leaf/internal)
/// - 4 bytes: header page index
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

    /// Migrated from java version.
    ///
    /// TODO: Figure out what this is used for, and if it's needed.
    old_data: Vec<u8>,
}

impl BTreeRootPointerPage {
    fn new(pid: &BTreePageID, bytes: &[u8]) -> Self {
        let mut reader = Cursor::new(bytes);

        // read page category
        let page_category = PageCategory::decode_from(&mut reader);
        if page_category != PageCategory::RootPointer {
            panic!("invalid page category: {:?}", page_category);
        }

        // read root page index
        let root_page_index = u32::decode_from(&mut reader);

        // read root page category
        let root_page_category = PageCategory::decode_from(&mut reader);

        // read header page index
        let header_page_index = u32::decode_from(&mut reader);

        let root_pid = BTreePageID {
            category: root_page_category,
            page_index: root_page_index,
            table_id: pid.get_table_id(),
        };

        let mut instance = Self {
            base: BTreeBasePage::new(pid),
            root_pid,
            header_page_index,
            old_data: Vec::new(),
        };

        instance.set_before_image();
        return instance;
    }

    pub fn new_empty_page(pid: &BTreePageID) -> Self {
        // set the root pid to 1
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: 1,
            table_id: pid.get_table_id(),
        };

        Self {
            base: BTreeBasePage::new(pid),
            root_pid,
            header_page_index: EMPTY_PAGE_ID,
            old_data: Vec::new(),
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
    fn new(pid: &BTreePageID, bytes: &[u8], _tuple_scheme: &Schema) -> Self {
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
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        writer.write(&self.get_pid().category);

        // write root page index
        writer.write(&self.root_pid.page_index);

        // write root page category
        writer.write(&self.root_pid.category);

        // write header page index
        writer.write(&self.header_page_index);

        return writer.to_padded_bytes(BufferPool::get_page_size());
    }

    fn set_before_image(&mut self) {
        self.old_data = self.get_page_data();
    }

    fn get_before_image(&self) -> Vec<u8> {
        if self.old_data.is_empty() {
            panic!("no before image");
        }
        return self.old_data.clone();
    }

    fn peek(&self) {
        debug!("BTreeRootPointerPage {{");
        debug!("  pid: {:?}", self.get_pid());
        debug!("  root_pid: {:?}", self.root_pid);
        debug!("  header_page_index: {}", self.header_page_index);
        debug!("}}");
    }
}
