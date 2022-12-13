use bit_vec::BitVec;

use super::{BTreeBasePage, BTreePage, BTreePageID, PageCategory};
use crate::{
    btree::tuple::TupleScheme,
    io::{SmallReader, Vaporizable},
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
}

impl BTreeHeaderPage {
    pub fn new(pid: &BTreePageID, bytes: Vec<u8>) -> BTreeHeaderPage {
        let mut reader = SmallReader::new(&bytes);

        // read page category
        let page_category = PageCategory::read_from(&mut reader);
        if page_category != PageCategory::Header {
            panic!("invalid page category: {:?}", page_category);
        }

        // read header
        let header = BitVec::read_from(&mut reader);

        let slot_count = header.len();

        BTreeHeaderPage {
            base: BTreeBasePage::new(pid),
            header,
            slot_count,
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
        unimplemented!()
    }
}
