use std::io::Cursor;

use super::{BTreePage, BTreePageID, BTreePageInit, PageCategory, FIRST_LEAF_PID};
use crate::{
    btree::buffer_pool::BufferPool,
    io::{Serializeable, SmallWriter},
    storage::table_schema::TableSchema,
};

/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: root page index
/// - 4 bytes: root page category (leaf/internal)
/// - 4 bytes: header page index
pub struct BTreeRootPointerPage {
    pid: BTreePageID,

    /// The type of this field is `BTreePageID` instead of
    /// `Option<BTreePageID>` because the root page is always
    /// present in the B+ tree.
    ///
    /// This decision also simplified the code.
    root_pid: BTreePageID,

    /// Migrated from old version.
    ///
    /// TODO: Figure out what this is used for, and if it's needed.
    old_data: Vec<u8>,
}

impl BTreeRootPointerPage {
    fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        let mut reader = Cursor::new(bytes);

        // read page category
        let page_category = PageCategory::decode(&mut reader, &());
        if page_category != PageCategory::RootPointer {
            panic!("invalid page category: {:?}", page_category);
        }

        // read root page index
        let root_page_index = u32::decode(&mut reader, &());

        // read root page category
        let root_page_category = PageCategory::decode(&mut reader, &());

        // read header page index
        let header_page_index = u32::decode(&mut reader, &());

        let root_pid = BTreePageID {
            category: root_page_category,
            page_index: root_page_index,
            table_id: pid.get_table_id(),
        };

        let mut instance = Self {
            pid: pid.clone(),
            root_pid,
            old_data: Vec::new(),
        };

        instance.set_before_image(table_schema);
        return instance;
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }
}

impl BTreePageInit for BTreeRootPointerPage {
    fn new_empty_page(pid: &BTreePageID, table_schema: &TableSchema) -> Self {
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: FIRST_LEAF_PID,
            table_id: pid.get_table_id(),
        };

        Self {
            pid: pid.clone(),
            root_pid,
            old_data: Vec::new(),
        }
    }
}

impl BTreePage for BTreeRootPointerPage {
    fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        Self::new(pid, bytes, table_schema)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid.clone()
    }

    fn get_page_data(&self, _table_schema: &TableSchema) -> Vec<u8> {
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        self.get_pid().category.encode(&mut writer, &());

        // write root page index
        self.root_pid.page_index.encode(&mut writer, &());

        // write root page category
        self.root_pid.category.encode(&mut writer, &());

        return writer.to_padded_bytes(BufferPool::get_page_size());
    }

    fn set_before_image(&mut self, table_schema: &TableSchema) {
        self.old_data = self.get_page_data(table_schema);
    }

    fn get_before_image(&self, _table_schema: &TableSchema) -> Vec<u8> {
        if self.old_data.is_empty() {
            panic!("no before image");
        }
        return self.old_data.clone();
    }
}
