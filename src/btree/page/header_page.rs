use std::io::Cursor;

use bit_vec::BitVec;
use log::debug;

use super::{BTreeBasePage, BTreePage, BTreePageID, PageCategory};
use crate::{
    btree::page_cache::PageCache,
    io::{Decodeable, SmallWriter},
    storage::schema::Schema,
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
    pub fn new(pid: &BTreePageID, bytes: &[u8]) -> BTreeHeaderPage {
        let mut instance: Self;

        if BTreeBasePage::is_empty_page(&bytes) {
            instance = Self::new_empty_page(pid);
        } else {
            // let mut reader = Cursor::new(bytes);
            let mut reader = Cursor::new(bytes);

            // read page category
            let page_category = PageCategory::decode_from(&mut reader);
            if page_category != PageCategory::Header {
                panic!("invalid page category: {:?}", page_category);
            }

            // read header
            let header = BitVec::decode_from(&mut reader);

            let slot_count = header.len();

            instance = BTreeHeaderPage {
                base: BTreeBasePage::new(pid),
                header,
                slot_count,
                old_data: Vec::new(),
            };
        }

        instance.set_before_image();
        return instance;
    }

    pub fn new_empty_page(pid: &BTreePageID) -> BTreeHeaderPage {
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
    pub fn mark_slot_status(
        &mut self,
        slot_index: usize,
        used: bool,
    ) {
        self.header.set(slot_index, used);
    }

    pub fn get_slots_count(&self) -> usize {
        self.slot_count
    }

    pub fn get_empty_slot(&self) -> Option<u32> {
        for i in 0..self.slot_count {
            if !self.header[i] {
                return Some(i as u32);
            }
        }
        None
    }
}

impl BTreePage for BTreeHeaderPage {
    fn new(
        pid: &BTreePageID,
        bytes: &[u8],
        _tuple_scheme: &Schema,
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
        let mut writer = SmallWriter::new();

        // write page category
        writer.write(&self.get_pid().category);

        // write header
        writer.write(&self.header);

        return writer.to_padded_bytes(PageCache::get_page_size());
    }

    fn set_before_image(&mut self) {
        self.old_data = self.get_page_data();
    }

    fn get_before_image(&self) -> Vec<u8> {
        if self.old_data.is_empty() {
            panic!("before image is not set");
        }
        return self.old_data.clone();
    }

    fn peek(&self) {
        debug!("header page: {:?}", self.get_pid())
    }
}
