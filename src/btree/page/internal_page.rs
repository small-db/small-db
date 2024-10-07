use std::{fmt, io::Cursor};

use bit_vec::BitVec;
use log::{debug, error};

use super::{BTreePage, BTreePageID, BTreePageInit, PageCategory};
use crate::{
    btree::{buffer_pool::BufferPool, consts::INDEX_SIZE},
    error::SmallError,
    io::{Serializeable, SmallWriter},
    storage::{table_schema::TableSchema, tuple::Cell},
    transaction::{Permission, Transaction},
    types::SmallResult,
    utils::{ceil_div, floor_div, HandyRwLock},
};

/// The internal page is used to store the keys and the page id of the
/// children.
///
/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: parent page index
/// - 4 bytes: children category (leaf/internal)
/// - n bytes: header bytes, indicate whether every slot of the page is used or
///   not.
/// - n bytes: keys
/// - n bytes: children
///
/// # Stable Criteria
///
/// count(used_slots) >= floor_dev(slot_count, 2)
pub struct BTreeInternalPage {
    pid: BTreePageID,

    keys: Vec<Cell>,

    /// Store the page id of the children.
    ///
    /// The size of this vector is always equal to `slot_count`. The
    /// unused slots will be filled with a dummy value. (The concrete
    /// value is not important, since it will never be used.)
    ///
    /// The right child of the nth entry is stored in the n-th slot.
    ///
    /// Note that the left child of the nth entry is not always
    /// locate in the n-1 slot, but the nearest left slot which
    /// has been marked as used.
    ///
    /// e.g:
    /// slots:    | 0     | 1     | 2    |
    /// header:   | true  | false | true |
    /// keys:     | dummy | dummy | key3 |
    /// children: | page1 | dummy | page3|
    ///
    /// For the above example, there is only one entry in the page,
    /// and the left child of the entry is page1, the right child
    /// is page3.
    ///
    /// The `dummy` value is ignored, and the children[0] is only
    /// used to store the left child of the first entry.
    children: Vec<BTreePageID>,

    /// The number of slots in the page, including the empty slots.
    ///
    /// Also including the dummy slots. (So the capacity of entries
    /// is `slot_count - 1`.)
    ///
    /// This filed should never be changed after the page is created.
    slot_count: usize,

    /// The header is used to indicate the status of each slot.
    ///
    /// The size of `header` is always equal to `slot_count`.
    ///
    /// The bytes size of `header` should be `ceiling(slot_count /
    /// 8)`.
    header: BitVec<u32>,

    children_category: PageCategory,

    old_data: Vec<u8>,
}

impl BTreeInternalPage {
    fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        let mut instance: Self;

        let slot_count = Self::get_children_cap(table_schema);

        let mut reader = Cursor::new(bytes);

        // read page category
        let category = PageCategory::decode(&mut reader, &());
        if category != PageCategory::Internal {
            panic!(
                "The page category of the internal page is not
                correct, expect: {:?}, actual: {:?}",
                PageCategory::Internal,
                category,
            );
        }

        // read children category
        let children_category = PageCategory::decode(&mut reader, &());

        // read header
        let header = BitVec::decode(&mut reader, &());

        // read keys
        let mut keys: Vec<Cell> = Vec::new();
        keys.push(Cell::Int64(0));
        for _ in 1..slot_count {
            let key = i64::decode(&mut reader, &());
            keys.push(Cell::Int64(key));
        }

        // read children
        let mut children: Vec<BTreePageID> = Vec::new();
        for _ in 0..slot_count {
            let child = BTreePageID::new(
                children_category,
                pid.get_table_id(),
                u32::decode(&mut reader, &()),
            );
            children.push(child);
        }

        instance = Self {
            pid: pid.clone(),
            keys,
            children,
            slot_count,
            header,
            children_category,
            old_data: Vec::new(),
        };

        instance.set_before_image(table_schema);
        return instance;
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

        // not found in the page, maybe it's a edge entry (half of the
        // entry in the sibling page)
        entry
    }

    pub fn stable(&self) -> bool {
        self.children_count() >= self.get_stable_threshold()
    }

    pub fn get_entry(&self, index: usize) -> Option<Entry> {
        if self.is_slot_used(index) {
            Some(Entry::new(
                &self.keys[index],
                &self.children[index - 1],
                &self.children[index],
            ))
        } else {
            None
        }
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

    /// Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        if slot_index >= self.slot_count {
            error!(
                "slot index out of range, slot index: {}, slot count: {}",
                slot_index, self.slot_count
            );
        }
        self.header[slot_index]
    }

    fn move_entry(&mut self, from: usize, to: usize) {
        if self.is_slot_used(from) && !self.is_slot_used(to) {
            self.keys[to] = self.keys[from].clone();

            // note that we don't need to update the left child slot,
            // since the left child slot is not the
            // nearest left slot, but the nearest
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

    // Get pid of the ith child. If there is no ith child, return
    // None.
    //
    // # Arguments
    //
    // * `index` - the index of the child, -1 means the rightmost child
    pub fn get_child_pid(&self, _index: usize) -> Option<BTreePageID> {
        unimplemented!()
    }

    pub fn get_first_child_pid(&self) -> BTreePageID {
        let mut it = BTreeInternalPageIterator::new(self);
        return it.next().unwrap().get_left_child();
    }

    pub fn get_last_child_pid(&self) -> BTreePageID {
        let mut it = BTreeInternalPageIterator::new(self);
        return it.next_back().unwrap().get_right_child();
    }

    pub fn get_left_sibling_pid(&self, tx: &Transaction) -> Option<BTreePageID> {
        let parent_pid = self.get_parent_pid();

        let parent_rc =
            BufferPool::get_internal_page(tx, Permission::ReadOnly, &parent_pid).unwrap();

        let parent = parent_rc.rl();
        let it = BTreeInternalPageIterator::new(&parent);
        for e in it {
            if e.get_right_child() == self.get_pid() {
                return Some(e.get_left_child());
            }
        }
        return None;
    }

    pub fn get_right_sibling_pid(&self, tx: &Transaction) -> Option<BTreePageID> {
        let parent_pid = self.get_parent_pid();

        let parent_rc =
            BufferPool::get_internal_page(tx, Permission::ReadOnly, &parent_pid).unwrap();

        let parent = parent_rc.rl();
        let it = BTreeInternalPageIterator::new(&parent);
        for e in it {
            if e.get_left_child() == self.get_pid() {
                return Some(e.get_right_child());
            }
        }
        return None;
    }

    pub fn get_entry_by_children(
        &self,
        left_pid: &BTreePageID,
        right_pid: &BTreePageID,
    ) -> Option<Entry> {
        let it = BTreeInternalPageIterator::new(self);
        for entry in it {
            if entry.get_left_child() == *left_pid && entry.get_right_child() == *right_pid {
                return Some(entry);
            }
        }
        None
    }

    pub fn check_integrity(
        &self,
        parent_pid: &BTreePageID,
        lower_bound: &Option<Cell>,
        upper_bound: &Option<Cell>,
        check_occupancy: bool,
        depth: usize,
    ) -> SmallResult {
        assert_eq!(self.get_pid().category, PageCategory::Internal);
        assert_eq!(&self.get_parent_pid(), parent_pid);

        let mut previous = lower_bound.clone();
        let it = BTreeInternalPageIterator::new(self);
        for e in it {
            if let Some(previous) = previous {
                if previous > e.get_key() {
                    let err_msg = format!(
                        "entries are not in order, previous (lower_bound): {:?}, current entry: {}, current pid: {}, parent pid: {}",
                        previous,
                        e,
                        self.get_pid(),
                        self.get_parent_pid(),
                    );
                    return Err(SmallError::new(&err_msg));
                }
            }
            previous = Some(e.get_key());
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                assert!(previous <= upper_bound.clone());
            }
        }

        if check_occupancy && depth > 0 {
            if self.children_count() < self.slot_count / 2 {
                let err_msg = format!(
                    "children count: {}, max children: {}, pid: {:?}",
                    self.children_count(),
                    self.slot_count / 2,
                    self.get_pid(),
                );
                return Err(SmallError::new(&err_msg));
            }
        }

        Ok(())
    }
}

// Insertion methods.
impl BTreeInternalPage {
    pub fn insert_entry(&mut self, e: &Entry) -> SmallResult {
        if self.empty_slots_count() == 0 {
            return Err(SmallError::new("No empty slots on this page."));
        }

        // check if this is the first entry
        if self.entries_count() == 0 {
            // reset the `children_category`
            self.children_category = e.get_left_child().category;

            // add the entry to the first slot (slot 1)
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

        // find the child pointer matching the left or right child in
        // this entry
        let mut slot_just_ahead: usize = usize::MAX;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                continue;
            }

            // circumstances 1: we want to insert a entry just after
            // the current entry
            if self.children[i] == e.get_left_child() {
                slot_just_ahead = i;
                break;
            }

            // circumstances 2: we want to insert a entry just inside
            // the current entry, so the right child of
            // the current entry should be updated to the
            // left child of the new entry
            if self.children[i] == e.get_right_child() {
                slot_just_ahead = i;
                // update right child of current entry
                self.children[i] = e.get_left_child();
                break;
            }
        }

        if slot_just_ahead == usize::MAX {
            let err = SmallError::new(&format!(
                "No slot found for entry {}, pid: {}, entries count: {}",
                e,
                self.get_pid(),
                self.entries_count()
            ));
            let iter = BTreeInternalPageIterator::new(self);
            debug!("page entries: {:?}", iter.collect::<Vec<Entry>>());
            return Err(err);
        }

        // shift entries back or forward to fill empty slot and make
        // room for new entry while keeping entries in sorted
        // order
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
}

// Methods for accessing dynamic attributes.
impl BTreeInternalPage {
    /// Empty slots (entries/children) count.
    pub fn empty_slots_count(&self) -> usize {
        let mut count = 0;
        // start from 1 because the first key slot is not used
        // since a page with m keys has m+1 pointers
        for i in 1..self.slot_count {
            if !self.is_slot_used(i) {
                count += 1
            }
        }
        count
    }

    pub fn children_count(&self) -> usize {
        let children_count = self.slot_count - self.empty_slots_count();

        // The minimum number of children is 2. (Since a single child
        // cannot form an entry.)
        if children_count < 2 {
            return 0;
        }

        children_count
    }

    pub fn entries_count(&self) -> usize {
        self.slot_count - self.empty_slots_count() - 1
    }

    /// Get the minimum number of children needed to keep this page stable.
    ///
    /// "floor division" vs "ceiling division":
    /// - they are the same when the "slot count" is even
    /// - when the "slot count" is odd, the "ceiling division" will mark the current
    ///  page as unstable when "used_slots == floor_dev(slot_count, 2)" and make it
    ///  rebalance or merge with its sibling.
    pub fn get_stable_threshold(&self) -> usize {
        // what if "self" is the root page?
        todo!();

        ceil_div(self.slot_count, 2)
    }
}

/// Associated functions.
impl BTreeInternalPage {
    pub fn get_children_capacity(&self) -> usize {
        self.slot_count
    }

    /// Get the capacity of children (pages) in this page. The
    /// capacity of entries is one less than it.
    pub fn get_children_cap(schema: &TableSchema) -> usize {
        let key_size = schema.get_pkey().get_type().get_disk_size();

        let bits_per_entry_including_header = key_size * 8 + INDEX_SIZE * 8 + 1;

        // extraBits:
        // - page category
        // - one parent pointer
        // - child page category
        // - one extra child pointer (page with m entries has m+1 pointers to children)
        // - header size
        // - 1 bit for extra header (for the slot 0)
        let extra_bits = (4 * INDEX_SIZE + 2) * 8 + 1;

        let entries_per_page =
            (BufferPool::get_page_size() * 8 - extra_bits) / bits_per_entry_including_header; // round down
        return entries_per_page + 1;
    }
}

impl BTreePageInit for BTreeInternalPage {
    fn new_empty_page(pid: &BTreePageID, schema: &TableSchema) -> Self {
        let slot_count = Self::get_children_cap(schema);

        // init empty header
        let mut header = BitVec::new();
        header.grow(slot_count, false);

        // init empty keys
        let mut keys = Vec::new();
        for _ in 0..slot_count {
            keys.push(Cell::Int64(0));
        }

        // init empty children
        let mut children = Vec::new();
        for _ in 0..slot_count {
            children.push(BTreePageID::new(PageCategory::Leaf, pid.get_table_id(), 0));
        }

        Self {
            pid: pid.clone(),
            keys,
            children,
            slot_count,
            header,
            children_category: PageCategory::Leaf,
            old_data: Vec::new(),
        }
    }
}

impl BTreePage for BTreeInternalPage {
    fn new(pid: &BTreePageID, bytes: &[u8], schema: &TableSchema) -> Self {
        Self::new(pid, bytes, schema)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid.clone()
    }

    fn get_page_data(&self, table_schema: &TableSchema) -> Vec<u8> {
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        self.get_pid().category.encode(&mut writer, &());

        // write children category
        self.children_category.encode(&mut writer, &());

        // write header
        self.header.encode(&mut writer, &());

        // write keys
        let t = table_schema.get_pkey().get_type();
        for i in 1..self.slot_count {
            self.keys[i].encode(&mut writer, &t);
        }

        // write children
        for i in 0..self.slot_count {
            self.children[i].page_index.encode(&mut writer, &());
        }

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

// All of the entries or tuples in the left child page should be less
// than or equal to the key, and all of the entries or tuples in the
// right child page should be greater than or equal to the key.
#[derive(Clone, Debug)]
pub struct Entry {
    key: Cell,
    left: BTreePageID,
    right: BTreePageID,

    // record position in the page
    record_id: usize,
}

impl Entry {
    pub fn new(key: &Cell, left: &BTreePageID, right: &BTreePageID) -> Self {
        Self {
            key: key.clone(),
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

    pub fn get_key(&self) -> Cell {
        self.key.clone()
    }

    pub fn set_key(&mut self, key: Cell) {
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
        write!(f, "({:?}, {}, {})", self.key, self.left, self.right)
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
            if right_child_position == 0 {
                break;
            }

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
                &self.page.keys[cursor],
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
                    &self.page.keys[self.right_child_position],
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
