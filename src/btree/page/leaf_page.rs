use bit_vec::BitVec;

use crate::{
    btree::{
        buffer_pool::BufferPool,
        tuple::{TupleScheme, WrappedTuple},
    },
    Tuple,
};

use super::{BTreeBasePage, BTreePageID, PageCategory};
use std::{cell::RefCell, rc::Rc};

use log::debug;

use crate::field::IntField;

pub struct BTreeLeafPage {
    page: BTreeBasePage,

    pub slot_count: usize,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    pub tuple_scheme: TupleScheme,

    // use usize instead of Option<BTreePageID> to reduce memory footprint
    right_sibling_id: usize,
    left_sibling_id: usize,

    key_field: usize,
}

impl std::ops::Deref for BTreeLeafPage {
    type Target = BTreeBasePage;
    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl std::ops::DerefMut for BTreeLeafPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

impl BTreeLeafPage {
    pub fn new(
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
            page: BTreeBasePage::new(pid),
            slot_count,
            header: BitVec::from_bytes(&bytes[..header_size]),
            tuples,
            tuple_scheme: tuple_scheme.clone(),
            right_sibling_id: 0,
            left_sibling_id: 0,
            key_field,
        }
    }

    pub fn set_right_sibling_pid(&mut self, pid: Option<BTreePageID>) {
        match pid {
            Some(pid) => {
                self.right_sibling_id = pid.page_index;
            }
            None => {}
        }
    }

    pub fn get_right_sibling_pid(&self) -> Option<BTreePageID> {
        if self.right_sibling_id == 0 {
            return None;
        } else {
            return Some(BTreePageID::new(
                PageCategory::Leaf,
                self.get_pid().table_id,
                self.right_sibling_id,
            ));
        }
    }

    pub fn set_left_sibling_pid(&mut self, pid: Option<BTreePageID>) {
        match pid {
            Some(pid) => {
                self.left_sibling_id = pid.page_index;
            }
            None => {}
        }
    }

    pub fn get_left_sibling_pid(&self) -> Option<BTreePageID> {
        if self.left_sibling_id == 0 {
            return None;
        } else {
            return Some(BTreePageID::new(
                PageCategory::Leaf,
                self.get_pid().table_id,
                self.left_sibling_id,
            ));
        }
    }

    /**
    Retrieve the maximum number of tuples this page can hold.
    */
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

    pub fn should_merge(&self) -> bool {
        if self.get_parent_pid().category == PageCategory::RootPointer {
            return false;
        }

        let max_empty_slots = self.slot_count - self.slot_count / 2; // ceiling
        return self.empty_slots_count() > max_empty_slots;
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

    /**
    Adds the specified tuple to the page such that all records remain in
    sorted order; the tuple should be updated to reflect
    that it is now stored on this page.
    tuple: The tuple to add.
    */
    pub fn insert_tuple(&mut self, tuple: &Tuple) {
        // find the first empty slot
        let mut first_empty_slot: i32 = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i as i32;
                // debug!("first emply slot: {}", first_empty_slot);
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

        debug!(
            "good slot: {}, first: {}, last: {}",
            good_slot, first_empty_slot, last_less_slot
        );
    }

    // Move a tuple from one slot to another slot, destination must be empty
    fn move_tuple(&mut self, from: usize, to: usize) {
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

    /**
    Returns true if associated slot on this page is filled.
    */
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        self.header[slot_index]
    }

    /*
    mark the slot as empty/filled.
    */
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
        assert_eq!(self.get_pid().category, PageCategory::Leaf);
        assert_eq!(&self.get_parent_pid(), parent_pid);

        let mut previous = lower_bound;
        let it = BTreeLeafPageIterator::new(self);
        for tuple in it {
            if let Some(previous) = previous {
                assert!(previous <= tuple.get_field(self.key_field));
            }
            previous = Some(tuple.get_field(self.key_field));
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                assert!(previous <= upper_bound);
            }
        }

        if check_occupancy && depth > 0 {
            assert!(self.tuples_count() >= self.get_slots_count() / 2);
        }
    }
}

pub struct BTreeLeafPageIteratorRc {
    page: Rc<RefCell<BTreeLeafPage>>,
    cursor: usize,
}

impl BTreeLeafPageIteratorRc {
    pub fn new(page: Rc<RefCell<BTreeLeafPage>>) -> Self {
        Self { page, cursor: 0 }
    }
}

impl Iterator for BTreeLeafPageIteratorRc {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let page = (*self.page).borrow();
        while self.cursor < page.slot_count {
            if page.is_slot_used(self.cursor) {
                let tuple = page.tuples[self.cursor].clone();
                self.cursor += 1;
                return Some(WrappedTuple::new(
                    tuple,
                    self.cursor,
                    page.get_pid(),
                ));
            } else {
                self.cursor += 1;
            }
        }

        None
    }
}

pub struct BTreeLeafPageIterator<'page> {
    page: &'page BTreeLeafPage,
    cursor: usize,
}

impl<'page> BTreeLeafPageIterator<'page> {
    pub fn new(page: &'page BTreeLeafPage) -> Self {
        Self {
            page,
            cursor: page.slot_count,
        }
    }
}

impl<'page> Iterator for BTreeLeafPageIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.page.slot_count {
            self.cursor += 1;
            if self.page.is_slot_used(self.cursor) {
                let real_cursor = self.cursor - 1;
                let t = WrappedTuple::new(
                    self.page.tuples[real_cursor].clone(),
                    real_cursor,
                    self.page.get_pid(),
                );
                return Some(t);
            }
        }

        None
    }
}

pub struct BTreeLeafPageReverseIterator<'page> {
    page: &'page BTreeLeafPage,
    cursor: usize,
}

impl<'page> BTreeLeafPageReverseIterator<'page> {
    pub fn new(page: &'page BTreeLeafPage) -> Self {
        Self {
            page,
            cursor: page.slot_count,
        }
    }
}

impl<'page> Iterator for BTreeLeafPageReverseIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.cursor.checked_sub(1) {
                Some(cursor) => {
                    self.cursor = cursor;
                    if self.page.is_slot_used(cursor) {
                        let tuple = self.page.tuples[cursor].clone();
                        return Some(WrappedTuple::new(
                            tuple,
                            cursor,
                            self.page.get_pid(),
                        ));
                    }
                }
                None => {
                    return None;
                }
            }
        }
    }
}
