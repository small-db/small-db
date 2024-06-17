use std::io::Cursor;

use bit_vec::BitVec;

use super::{BTreeBasePage, BTreePage, BTreePageID, PageCategory};
use crate::{
    btree::buffer_pool::BufferPool,
    io::{Serializeable, SmallWriter},
    storage::table_schema::TableSchema,
};

/// # Binary Layout
///
/// - 4 bytes: page category
/// - n bytes: header
pub struct BTreeHeaderPage {
    base: BTreeBasePage,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    slot_count: usize,

    old_data: Vec<u8>,
}

impl BTreeHeaderPage {
    pub fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        let mut instance: Self;

        if BTreeBasePage::is_empty_page(&bytes) {
            instance = Self::new_empty_page(pid);
        } else {
            let mut reader = Cursor::new(bytes);

            // read page category
            let page_category = PageCategory::decode(&mut reader, &());
            if page_category != PageCategory::Header {
                panic!("invalid page category: {:?}", page_category);
            }

            // read header
            let header = BitVec::decode(&mut reader, &());

            let slot_count = header.len();

            instance = BTreeHeaderPage {
                base: BTreeBasePage::new(pid),
                header,
                slot_count,
                old_data: Vec::new(),
            };
        }

        instance.set_before_image(table_schema);
        return instance;
    }

    pub fn new_empty_page(pid: &BTreePageID) -> BTreeHeaderPage {
        // TODO: get slot_count dynamically
        // TODO: make header pages a linked list
        let slot_count = 1000;

        let mut header = BitVec::new();
        header.grow(slot_count, false);

        BTreeHeaderPage {
            base: BTreeBasePage::new(pid),
            header,
            slot_count,
            old_data: Vec::new(),
        }
    }

    // mark the slot as empty/filled.
    pub(crate) fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub(crate) fn get_slots_count(&self) -> usize {
        self.slot_count
    }

    pub(crate) fn get_empty_slot(&self) -> Option<u32> {
        for i in 0..self.slot_count {
            if !self.header[i] {
                return Some(i as u32);
            }
        }
        None
    }
}

impl BTreePage for BTreeHeaderPage {
    fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        Self::new(pid, bytes, table_schema)
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

    fn get_page_data(&self, _table_schema: &TableSchema) -> Vec<u8> {
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        self.get_pid().category.encode(&mut writer, &());

        // write header
        self.header.encode(&mut writer, &());

        return writer.to_padded_bytes(BufferPool::get_page_size());
    }

    fn set_before_image(&mut self, table_schema: &TableSchema) {
        self.old_data = self.get_page_data(table_schema);
    }

    fn get_before_image(&self, _table_schema: &TableSchema) -> Vec<u8> {
        if self.old_data.is_empty() {
            panic!("before image is not set");
        }
        return self.old_data.clone();
    }
}
