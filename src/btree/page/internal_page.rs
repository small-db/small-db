use std::{cell::RefCell, convert::TryInto, fmt, rc::Rc};

use bit_vec::BitVec;
use log::info;

use crate::{
    btree::{buffer_pool::BufferPool, consts::INDEX_SIZE, tuple::TupleScheme},
    field::{get_type_length, IntField},
};

use super::{BTreeBasePage, BTreePageID, PageCategory};

pub struct BTreeInternalPage {
    page: BTreeBasePage,

    pub keys: Vec<IntField>,
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

    pub fn dig(&self) {
        info!(
            "page id: {}, parent pid: {}",
            self.get_pid(),
            self.get_parent_pid()
        );
        info!("empty slot count: {}", self.empty_slots_count());
        info!("keys: {:?}", self.keys);
        info!("children: {:?}", self.children);
        let it = BTreeInternalPageIterator::new(self);
        for (i, e) in it.enumerate() {
            info!("{}: {}", i, e);
        }
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

    pub fn delete_entry(&mut self, index: usize) {
        self.mark_slot_status(index, false);
    }

    /**
    Returns true if associated slot on this page is filled.
    */
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        self.header[slot_index]
    }

    pub fn insert_entry(&mut self, e: &Entry) {
        // if this is the first entry, add it and return
        if self.empty_slots_count() == Self::get_max_entries(4) {
            self.children[0] = e.get_left_child();
            self.children[1] = e.get_right_child();
            self.keys[1] = e.get_key();
            self.mark_slot_status(0, true);
            self.mark_slot_status(1, true);
            return;
        }

        // find the first empty slot, start from 1
        let mut empty_slot: i32 = -1;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                empty_slot = i as i32;
                break;
            }
        }

        // if there is no empty slot, return
        if empty_slot == -1 {
            panic!("no empty slot");
        }

        // find the child pointer matching the left or right child in this entry
        let mut less_or_eq_slot = -1;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                continue;
            }

            if self.children[i] == e.get_left_child() {
                // gotcha
                less_or_eq_slot = i as i32;

                // we not break here, but break on the next iteration
                // to validate the keys is in order
                continue;
            }

            if self.children[i] == e.get_right_child() {
                // gotcha
                less_or_eq_slot = i as i32;

                // update right child of current entry
                self.children[i] = e.get_left_child();

                // we not break here, but break on the next iteration
                // to validate the keys is in order
                continue;
            }

            // validate that the next key is greater than or equal to the one we
            // are inserting
            if less_or_eq_slot != -1 {
                if self.keys[i] < e.get_key() {
                    panic!("key is not in order");
                }
                break;
            }
        }

        if less_or_eq_slot == -1 {
            info!("you are try to insert: {}", e);
            info!("page id: {}", self.get_pid());
            panic!("no less or equal slot",);
        }

        // shift entries back or forward to fill empty slot and make room for
        // new entry while keeping entries in sorted order
        let good_slot: i32;
        if empty_slot < less_or_eq_slot {
            for i in empty_slot..less_or_eq_slot {
                self.move_entry((i + 1) as usize, i as usize);
            }
            good_slot = less_or_eq_slot
        } else {
            for i in (less_or_eq_slot + 1..empty_slot).rev() {
                self.move_entry(i as usize, i as usize + 1);
            }
            good_slot = less_or_eq_slot + 1
        }

        self.keys[good_slot as usize] = e.get_key();
        self.children[good_slot as usize] = e.get_right_child();
        self.mark_slot_status(good_slot as usize, true);
    }

    fn move_entry(&mut self, from: usize, to: usize) {
        if self.is_slot_used(from) && !self.is_slot_used(to) {
            self.keys[to] = self.keys[from];
            self.children[to] = self.children[from];
            self.children[to - 1] = self.children[from - 1];
            self.mark_slot_status(from, false);
            self.mark_slot_status(to, true);
        } else {
            panic!("move_entry: invalid slot, from: {}, to: {}", from, to);
        }
    }

    fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
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
        assert_eq!(self.get_pid().category, PageCategory::Internal);
        assert_eq!(&self.get_parent_pid(), parent_pid);

        let mut previous = lower_bound;
        let it = BTreeInternalPageIterator::new(self);
        for e in it {
            if let Some(previous) = previous {
                assert!(previous <= e.get_key());
            }
            previous = Some(e.get_key());
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                assert!(previous <= upper_bound);
            }
        }

        if check_occupancy && depth > 0 {
            assert!(self.entries_count() >= Self::get_max_entries(4) / 2);
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
}

impl<'page> BTreeInternalPageIterator<'page> {
    pub fn new(page: &'page BTreeInternalPage) -> Self {
        Self { page, cursor: 0 }
    }
}

impl Iterator for BTreeInternalPageIterator<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.cursor += 1;
            if self.cursor >= self.page.slot_count {
                return None;
            }

            if !self.page.is_slot_used(self.cursor) {
                continue;
            }
            return Some(Entry::new(
                self.page.keys[self.cursor],
                &self.page.children[self.cursor - 1],
                &self.page.children[self.cursor],
            ));
        }
    }
}

pub struct BTreeInternalPageReverseIterator<'page> {
    page: &'page BTreeInternalPage,
    cursor: usize,
}

impl<'page> BTreeInternalPageReverseIterator<'page> {
    pub fn new(page: &'page BTreeInternalPage) -> Self {
        Self {
            page,
            cursor: page.slot_count,
        }
    }
}

impl Iterator for BTreeInternalPageReverseIterator<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.cursor -= 1;

            // entries start from 1
            if self.cursor < 1 {
                return None;
            }

            if !self.page.is_slot_used(self.cursor) {
                continue;
            }
            let mut e = Entry::new(
                self.page.keys[self.cursor],
                &self.page.children[self.cursor - 1],
                &self.page.children[self.cursor],
            );
            e.set_record_id(self.cursor);
            return Some(e);
        }
    }
}
