use bit_vec::BitVec;

use crate::btree::tuple::TupleScheme;

use super::{BTreeBasePage, BTreePage, BTreePageID};

pub struct BTreeHeaderPage {
    base: BTreeBasePage,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    slot_count: usize,
}

impl BTreeHeaderPage {
    pub fn new(pid: &BTreePageID) -> BTreeHeaderPage {
        let header_size = 100;
        let slot_count = 100 * 8;
        let bytes: Vec<u8> = vec![0xff; header_size];
        let header = BitVec::from_bytes(&bytes);

        BTreeHeaderPage {
            base: BTreeBasePage::new(pid),
            header: header,
            slot_count: slot_count,
        }
    }

    // mark the slot as empty/filled.
    pub fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub fn get_slots_count(&self) -> usize {
        self.slot_count
    }

    pub fn get_empty_slot(&self) -> Option<usize> {
        for i in 0..self.slot_count {
            if !self.header[i] {
                return Some(i);
            }
        }
        None
    }
}

impl BTreePage for BTreeHeaderPage {
    fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self {
        Self::new(pid)
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
}
