use crate::error::MyError;
use std::fmt;

use bit_vec::BitVec;
use log::{error, info};

use crate::{
    btree::{buffer_pool::BufferPool, consts::INDEX_SIZE, tuple::TupleScheme},
    field::{get_type_length, IntField},
};

use super::{
    BTreeBasePage, BTreeLeafPage, BTreePage, BTreePageID, PageCategory,
};

pub struct BTreeInternalPage {
    page: BTreeBasePage,

    pub keys: Vec<IntField>,

    /// note: the left child of the nth `entry` is not always locate in
    /// the n-1 slot, but the nearest left slot which has been marked
    /// as used.
    pub children: Vec<BTreePageID>,

    slot_count: usize,

    // header bytes
    header: BitVec<u32>,

    tuple_scheme: TupleScheme,

    key_field: usize,
}

impl std::ops::Deref for BTreeInternalPage {
    type Target = BTreeBasePage;
    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl std::ops::DerefMut for BTreeInternalPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

impl BTreeInternalPage {
    pub fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self {
        let key_size =
            get_type_length(tuple_scheme.fields[key_field].field_type);
        let slot_count = Self::get_max_entries(key_size) + 1;
        let header_size = Self::get_header_size(slot_count) as usize;

        let mut keys: Vec<IntField> = Vec::new();
        let mut children: Vec<BTreePageID> = Vec::new();
        keys.resize(slot_count, IntField::new(0));
        children.resize(slot_count, BTreePageID::new(PageCategory::Leaf, 0, 0));

        Self {
            page: BTreeBasePage::new(pid),
            keys,
            children,
            slot_count,
            header: BitVec::from_bytes(&bytes[..header_size]),
            tuple_scheme: tuple_scheme.clone(),
            key_field,
        }
    }

    pub fn get_coresponding_entry(
        &self,
        left_pid: Option<&BTreePageID>,
        right_pid: Option<&BTreePageID>,
    ) -> Option<Entry> {
        let mut it = BTreeInternalPageIterator::new(self);
        let mut entry = None;
        for e in it.by_ref() {
            if let Some(left) = left_pid {
                if e.get_left_child() != *left {
                    continue;
                }
            }
            if let Some(right) = right_pid {
                if e.get_right_child() != *right {
                    continue;
                }
            }

            entry = Some(e);
            break;
        }

        // not found in the page, maybe it's a edge entry (half of the entry
        // in the sibling page)
        entry
    }

    pub fn stable(&self) -> bool {
        if self.get_parent_pid().category == PageCategory::RootPointer {
            return true;
        }

        let max_empty_slots = self.slot_count - self.slot_count / 2; // ceiling
        return self.empty_slots_count() <= max_empty_slots;
    }

    fn get_header_size(max_entries_count: usize) -> usize {
        // +1 for children
        let slots = max_entries_count + 1;
        slots / 8 + 1
    }

    /**
    Retrieve the maximum number of entries this page can hold. (The number of keys)
    */
    pub fn get_max_entries(key_size: usize) -> usize {
        let bits_per_entry_including_header = key_size * 8 + INDEX_SIZE * 8 + 1;
        /*
        extraBits are: one parent pointer, 1 byte for child page category,
        one extra child pointer (node with m entries has m+1 pointers to
        children),
        1 bit for extra header (why?)
        */
        let extra_bits = 2 * INDEX_SIZE * 8 + 8;
        let entries_per_page = (BufferPool::get_page_size() * 8 - extra_bits)
            / bits_per_entry_including_header; //round down
        return entries_per_page;
    }

    pub fn get_page_id(&self) -> BTreePageID {
        self.get_pid()
    }

    pub fn get_entry(&self, index: usize) -> Option<Entry> {
        if self.is_slot_used(index) {
            Some(Entry::new(
                self.keys[index],
                &self.children[index - 1],
                &self.children[index],
            ))
        } else {
            None
        }
    }

    pub fn empty_slots_count(&self) -> usize {
        let mut count = 0;
        // start from 1 because the first key slot is not used
        // since a node with m keys has m+1 pointers
        for i in 1..self.slot_count {
            if !self.is_slot_used(i) {
                count += 1
            }
        }
        count
    }

    pub fn entries_count(&self) -> usize {
        self.slot_count - self.empty_slots_count() - 1
    }

    pub fn delete_key_and_right_child(&mut self, record_id: usize) {
        self.mark_slot_status(record_id, false);
    }

    pub fn delete_key_and_left_child(&mut self, record_id: usize) {
        for i in (0..record_id).rev() {
            if self.is_slot_used(i) {
                // why?
                self.children[i] = self.children[record_id];

                self.mark_slot_status(record_id, false);
                return;
            }
        }
    }

    pub fn update_entry(&mut self, entry: &Entry) {
        let record_id = entry.get_record_id();

        // set left child
        for i in (0..record_id).rev() {
            if self.is_slot_used(i) {
                self.children[i] = entry.get_left_child();
                break;
            }
        }

        self.children[record_id] = entry.get_right_child();
        self.keys[record_id] = entry.get_key();
    }

    /**
    Returns true if associated slot on this page is filled.
    */
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        self.header[slot_index]
    }

    pub fn insert_entry(&mut self, e: &Entry) -> Result<(), MyError> {
        if self.empty_slots_count() == 0 {
            return Err(MyError::new("No empty slots on this page."));
        }

        // if this is the first entry, add it and return
        if self.empty_slots_count() == Self::get_max_entries(4) {
            self.children[0] = e.get_left_child();
            self.children[1] = e.get_right_child();
            self.keys[1] = e.get_key();
            self.mark_slot_status(0, true);
            self.mark_slot_status(1, true);
            return Ok(());
        }

        // find the first empty slot, start from 1
        let mut empty_slot = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                empty_slot = i;
                break;
            }
        }

        // find the child pointer matching the left or right child in this entry
        let mut slot_just_ahead: usize = usize::MAX;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                continue;
            }

            // circumstances 1: we want to insert a entry just after the current
            // entry
            if self.children[i] == e.get_left_child() {
                slot_just_ahead = i;
                break;
            }

            // circumstances 2: we want to insert a entry just inside the
            // current entry, so the right child of the current
            // entry should be updated to the left child of the new
            // entry
            if self.children[i] == e.get_right_child() {
                slot_just_ahead = i;
                // update right child of current entry
                self.children[i] = e.get_left_child();
                break;
            }
        }

        if slot_just_ahead == usize::MAX {
            let e = MyError::new(&format!(
                "No slot found for entry {}, pid: {}, entries count: {}",
                e,
                self.get_pid(),
                self.entries_count()
            ));
            error!("{}", e);
            panic!(e);
            return Err(e);
        }

        // shift entries back or forward to fill empty slot and make room for
        // new entry while keeping entries in sorted order
        let good_slot: usize;
        if empty_slot < slot_just_ahead {
            for i in empty_slot..slot_just_ahead {
                self.move_entry(i + 1, i);
            }
            good_slot = slot_just_ahead
        } else {
            for i in (slot_just_ahead + 1..empty_slot).rev() {
                self.move_entry(i, i + 1);
            }
            good_slot = slot_just_ahead + 1
        }

        self.keys[good_slot] = e.get_key();
        self.children[good_slot] = e.get_right_child();
        self.mark_slot_status(good_slot, true);
        Ok(())
    }

    fn move_entry(&mut self, from: usize, to: usize) {
        if self.is_slot_used(from) && !self.is_slot_used(to) {
            self.keys[to] = self.keys[from];

            // note that we don't need to update the left child slot, since the
            // left child slot is not the nearest left slot, but the nearest
            // `used` slot, so it should be kept untouched
            self.children[to] = self.children[from];

            self.mark_slot_status(from, false);
            self.mark_slot_status(to, true);
        } else {
            // there is hole in the middle of the page, just ignore it
        }
    }

    fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub fn get_first_child_pid(&self) -> BTreePageID {
        let mut it = BTreeInternalPageIterator::new(self);
        return it.next().unwrap().get_left_child();
    }

    pub fn get_last_child_pid(&self) -> BTreePageID {
        let mut it = BTreeInternalPageIterator::new(self);
        return it.next_back().unwrap().get_right_child();
    }

    pub fn check_integrity(
        &self,
        parent_pid: &BTreePageID,
        lower_bound: Option<IntField>,
        upper_bound: Option<IntField>,
        check_occupancy: bool,
        depth: usize,
    ) {
        assert_eq!(self.get_pid().category, PageCategory::Internal);
        assert_eq!(&self.get_parent_pid(), parent_pid);

        let mut previous = lower_bound;
        let it = BTreeInternalPageIterator::new(self);
        for e in it {
            if let Some(previous) = previous {
                assert!(
                    previous <= e.get_key(),
                    "entries are not in order, previous: {}, current: {}",
                    previous,
                    e,
                );
            }
            previous = Some(e.get_key());
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                assert!(previous <= upper_bound);
            }
        }

        if check_occupancy && depth > 0 {
            // minus 1 hear since the page may become lower than half full
            // in the process of splitting
            let minimal_stable = Self::get_max_entries(4) / 2 - 1;
            assert!(
                self.entries_count() >= minimal_stable,
                "entries count: {}, max entries: {}, pid: {}",
                self.entries_count(),
                Self::get_max_entries(4),
                self.get_pid(),
            );
        }
    }
}

/*
All of the entries or tuples in the left child page should be less than or equal to
the key, and all of the entries or tuples in the right child page should be greater
than or equal to the key.
*/
#[derive(Clone, Copy)]
pub struct Entry {
    key: IntField,
    left: BTreePageID,
    right: BTreePageID,

    // record position in the page
    record_id: usize,
}

impl Entry {
    pub fn new(key: IntField, left: &BTreePageID, right: &BTreePageID) -> Self {
        Self {
            key,
            left: *left,
            right: *right,

            record_id: 0,
        }
    }

    pub fn set_record_id(&mut self, record_id: usize) {
        self.record_id = record_id;
    }

    pub fn get_record_id(&self) -> usize {
        self.record_id
    }

    pub fn get_key(&self) -> IntField {
        self.key
    }

    pub fn set_key(&mut self, key: IntField) {
        self.key = key;
    }

    pub fn get_left_child(&self) -> BTreePageID {
        self.left
    }

    pub fn get_right_child(&self) -> BTreePageID {
        self.right
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.key, self.left, self.right)
    }
}

pub struct BTreeInternalPageIterator<'page> {
    page: &'page BTreeInternalPage,

    cursor: usize,
    left_child_position: usize,

    reverse_cursor: usize,
    right_child_position: usize,
}

impl<'page> BTreeInternalPageIterator<'page> {
    pub fn new(page: &'page BTreeInternalPage) -> Self {
        let mut right_child_position = page.slot_count;
        loop {
            right_child_position -= 1;
            if page.is_slot_used(right_child_position) {
                break;
            }
        }

        Self {
            page,

            cursor: 0,
            left_child_position: 0,

            reverse_cursor: right_child_position,
            right_child_position,
        }
    }
}

impl Iterator for BTreeInternalPageIterator<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.cursor += 1;
            let cursor = self.cursor;

            if cursor >= self.page.slot_count {
                return None;
            }

            if !self.page.is_slot_used(cursor) {
                continue;
            }
            let mut e = Entry::new(
                self.page.keys[cursor],
                &self.page.children[self.left_child_position],
                &self.page.children[cursor],
            );
            e.set_record_id(cursor);

            // set left child position for next iteration
            self.left_child_position = cursor;

            return Some(e);
        }
    }
}

impl<'page> DoubleEndedIterator for BTreeInternalPageIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(left_index) = self.reverse_cursor.checked_sub(1) {
                self.reverse_cursor = left_index;
                if !self.page.is_slot_used(left_index) {
                    continue;
                }

                let mut e = Entry::new(
                    self.page.keys[self.right_child_position],
                    &self.page.children[left_index],
                    &self.page.children[self.right_child_position],
                );
                e.set_record_id(self.right_child_position);

                // set right child position for next iteration
                self.right_child_position = left_index;

                return Some(e);
            } else {
                return None;
            }
        }
    }
}
