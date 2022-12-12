use std::sync::{Arc, RwLock};

use backtrace::Backtrace;
use bit_vec::BitVec;

use super::{
    BTreeBasePage, BTreePage, BTreePageID, PageCategory, EMPTY_PAGE_ID,
};
use crate::{
    btree::{
        buffer_pool::BufferPool,
        tuple::{TupleScheme, WrappedTuple},
    },
    field::IntField,
    utils::HandyRwLock,
    Tuple,
};

/// A leaf page in the B+ tree.
/// 
/// # Binary Layout
/// 
/// - 4 bytes: parent page index
/// - 4 bytes: left sibling page index
/// - 4 bytes: right sibling page index
/// - n bytes: 
pub struct BTreeLeafPage {
    base: BTreeBasePage,

    pub slot_count: usize,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    pub tuple_scheme: TupleScheme,

    // use u32 instead of Option<BTreePageID> to reduce memory footprint
    right_sibling_id: u32,
    left_sibling_id: u32,

    key_field: usize,
}

impl BTreeLeafPage {
    pub fn set_right_pid(&mut self, pid: Option<BTreePageID>) {
        match pid {
            Some(pid) => {
                self.right_sibling_id = pid.page_index;
            }
            None => {
                self.right_sibling_id = EMPTY_PAGE_ID;
            }
        }
    }

    pub fn get_right_pid(&self) -> Option<BTreePageID> {
        if self.right_sibling_id == EMPTY_PAGE_ID {
            return None;
        } else {
            return Some(BTreePageID::new(
                PageCategory::Leaf,
                self.get_pid().table_id,
                self.right_sibling_id,
            ));
        }
    }

    pub fn set_left_pid(&mut self, pid: Option<BTreePageID>) {
        match pid {
            Some(pid) => {
                self.left_sibling_id = pid.page_index;
            }
            None => {
                self.left_sibling_id = EMPTY_PAGE_ID;
            }
        }
    }

    pub fn get_left_pid(&self) -> Option<BTreePageID> {
        if self.left_sibling_id == EMPTY_PAGE_ID {
            return None;
        } else {
            return Some(BTreePageID::new(
                PageCategory::Leaf,
                self.get_pid().table_id,
                self.left_sibling_id,
            ));
        }
    }

    /// Retrieve the maximum number of tuples this page can hold.
    pub fn calculate_slots_count(scheme: &TupleScheme) -> usize {
        let bits_per_tuple_including_header = scheme.get_size() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent
        // pointer
        let index_size: usize = 4;
        let extra_bits = 3 * index_size * 8;
        (BufferPool::get_page_size() * 8 - extra_bits)
            / bits_per_tuple_including_header
    }

    pub fn get_slots_count(&self) -> usize {
        self.slot_count
    }

    /// stable means at least half of the page is occupied
    pub fn stable(&self) -> bool {
        if self.get_parent_pid().category == PageCategory::RootPointer {
            return true;
        }

        let stable_threshold = self.slot_count - self.slot_count / 2; // ceiling
        return self.tuples_count() >= stable_threshold;
    }

    pub fn empty_slots_count(&self) -> usize {
        let mut count = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                count += 1;
            }
        }
        count
    }

    /// Returns the number of tuples currently stored on this page
    pub fn tuples_count(&self) -> usize {
        self.slot_count - self.empty_slots_count()
    }

    // Computes the number of bytes in the header of
    // a page in a BTreeFile with each tuple occupying
    // tupleSize bytes
    pub fn calculate_header_size(slot_count: usize) -> usize {
        slot_count / 8 + 1
    }

    /// Adds the specified tuple to the page such that all records remain in
    /// sorted order; the tuple should be updated to reflect
    /// that it is now stored on this page.
    /// tuple: The tuple to add.
    pub fn insert_tuple(&mut self, tuple: &Tuple) {
        // find the first empty slot
        let mut first_empty_slot: i32 = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i as i32;
                break;
            }
        }

        // Find the last key less than or equal to the key being inserted.
        //
        // -1 indicate there is no such key less than tuple.key, so the tuple
        // should be inserted in slot 0 (-1 + 1).
        let mut last_less_slot: i32 = -1;
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                if self.tuples[i].get_field(self.key_field)
                    < tuple.get_field(self.key_field)
                {
                    last_less_slot = i as i32;
                } else {
                    break;
                }
            }
        }

        // shift records back or forward to fill empty slot and make room for
        // new record while keeping records in sorted order
        let good_slot: usize;
        if first_empty_slot < last_less_slot {
            for i in first_empty_slot..last_less_slot {
                self.move_tuple((i + 1) as usize, i as usize);
            }
            good_slot = last_less_slot as usize;
        } else {
            for i in (last_less_slot + 1..first_empty_slot).rev() {
                self.move_tuple(i as usize, (i + 1) as usize);
            }
            good_slot = (last_less_slot + 1) as usize;
        }

        // insert new record into the correct spot in sorted order
        self.tuples[good_slot] = tuple.clone();
        self.mark_slot_status(good_slot, true);
    }

    // Move a tuple from one slot to another slot, destination must be empty
    fn move_tuple(&mut self, from: usize, to: usize) {
        // return if the source slot is empty
        if !self.is_slot_used(from) {
            return;
        }

        self.tuples[to] = self.tuples[from].clone();
        self.mark_slot_status(to, true);
        self.mark_slot_status(from, false);
    }

    pub fn get_tuple(&self, slot_index: usize) -> Option<Tuple> {
        if self.is_slot_used(slot_index) {
            return Some(self.tuples[slot_index].clone());
        }
        None
    }

    pub fn delete_tuple(&mut self, slot_index: usize) {
        self.mark_slot_status(slot_index, false);
    }

    /// Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        self.header[slot_index]
    }

    // mark the slot as empty/filled.
    pub fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub fn check_integrity(
        &self,
        parent_pid: &BTreePageID,
        lower_bound: Option<IntField>,
        upper_bound: Option<IntField>,
        check_occupancy: bool,
        depth: usize,
    ) {
        let bt = Backtrace::new();

        assert_eq!(self.get_pid().category, PageCategory::Leaf);
        assert_eq!(
            &self.get_parent_pid(),
            parent_pid,
            "parent pid incorrect, current page: {:?}, current parent pid: {:?}, backtrace: {:?}",
            self.get_pid(),
            self.get_parent_pid(),
            bt,
        );

        let mut previous = lower_bound;
        let it = BTreeLeafPageIterator::new(self);
        for tuple in it {
            if let Some(previous) = previous {
                assert!(
                    previous <= tuple.get_field(self.key_field),
                    "previous: {:?}, current: {:?}, page_id: {:?}",
                    previous,
                    tuple.get_field(self.key_field),
                    self.get_pid(),
                );
            }
            previous = Some(tuple.get_field(self.key_field));
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                assert!(
                    previous <= upper_bound,
                    "the last tuple exceeds upper_bound, last tuple: {}, upper bound: {}",
                    previous,
                    upper_bound,
                );
            }
        }

        if check_occupancy && depth > 0 {
            assert!(self.tuples_count() >= self.get_slots_count() / 2);
        }
    }
}

impl BTreePage for BTreeLeafPage {
    fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self {
        let slot_count = Self::calculate_slots_count(&tuple_scheme);
        let header_size = Self::calculate_header_size(slot_count) as usize;

        // init tuples
        let mut tuples = Vec::new();
        for i in 0..slot_count {
            let start = header_size + i * tuple_scheme.get_size();
            let end = start + tuple_scheme.get_size();
            let t = Tuple::new(tuple_scheme.clone(), &bytes[start..end]);
            tuples.push(t);
        }

        Self {
            base: BTreeBasePage::new(pid),
            slot_count,
            header: BitVec::from_bytes(&bytes[..header_size]),
            tuples,
            tuple_scheme: tuple_scheme.clone(),
            right_sibling_id: EMPTY_PAGE_ID,
            left_sibling_id: EMPTY_PAGE_ID,
            key_field,
        }
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

        // write header
        let header_size = Self::calculate_header_size(self.slot_count) as usize;
        let header = self.header.to_bytes();
        data[..header_size].copy_from_slice(&header);

        // write tuples
        for i in 0..self.slot_count {
            let start = header_size + i * self.tuple_scheme.get_size();
            let end = start + self.tuple_scheme.get_size();
            let tuple = self.tuples[i].to_bytes();
            data[start..end].copy_from_slice(&tuple);
        }

        return data;
    }
}

pub struct BTreeLeafPageIteratorRc {
    page: Arc<RwLock<BTreeLeafPage>>,
    cursor: i32,
    reverse_cursor: i32,
}

impl BTreeLeafPageIteratorRc {
    pub fn new(page: Arc<RwLock<BTreeLeafPage>>) -> Self {
        let slot_count = page.rl().get_slots_count();
        Self {
            page,
            cursor: -1,
            reverse_cursor: slot_count as i32,
        }
    }
}

impl Iterator for BTreeLeafPageIteratorRc {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.page.rl();
        loop {
            self.cursor += 1;
            let cursor = self.cursor as usize;
            if cursor >= page.slot_count {
                return None;
            }

            if page.is_slot_used(cursor) {
                return Some(WrappedTuple::new(
                    page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}

impl DoubleEndedIterator for BTreeLeafPageIteratorRc {
    fn next_back(&mut self) -> Option<Self::Item> {
        let page = self.page.rl();
        loop {
            self.reverse_cursor -= 1;
            if self.reverse_cursor < 0 {
                return None;
            }

            let cursor = self.reverse_cursor as usize;
            if page.is_slot_used(cursor) {
                return Some(WrappedTuple::new(
                    page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}

pub struct BTreeLeafPageIterator<'page> {
    page: &'page BTreeLeafPage,
    cursor: i32,
    reverse_cursor: i32,
}

impl<'page> BTreeLeafPageIterator<'page> {
    pub fn new(page: &'page BTreeLeafPage) -> Self {
        Self {
            page,
            cursor: -1,
            reverse_cursor: page.slot_count as i32,
        }
    }
}

impl<'page> Iterator for BTreeLeafPageIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.page;
        loop {
            self.cursor += 1;
            let cursor = self.cursor as usize;
            if cursor >= page.slot_count {
                return None;
            }

            if page.is_slot_used(cursor) {
                return Some(WrappedTuple::new(
                    page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}

impl<'page> DoubleEndedIterator for BTreeLeafPageIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let page = self.page;
        loop {
            self.reverse_cursor -= 1;
            if self.reverse_cursor < 0 {
                return None;
            }

            let cursor = self.reverse_cursor as usize;
            if page.is_slot_used(cursor) {
                return Some(WrappedTuple::new(
                    page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}
