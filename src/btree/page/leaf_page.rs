use std::{
    fmt,
    io::Cursor,
    sync::{Arc, RwLock},
};

use bit_vec::BitVec;

use super::{BTreePage, BTreePageID, BTreePageInit, PageCategory, PageDebug, EMPTY_PID};
use crate::{
    btree::{buffer_pool::BufferPool, consts::INDEX_SIZE},
    error::SmallError,
    io::{read_into, Serializeable, SmallWriter},
    storage::{
        table_schema::TableSchema,
        tuple::{Cell, Tuple, WrappedTuple},
    },
    transaction::{Transaction, TransactionID},
    types::SmallResult,
    utils::{ceil_div, HandyRwLock},
    Predicate,
};

/// A leaf page in the B+ tree.
///
/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: parent page index
/// - 4 bytes: left sibling page index
/// - 4 bytes: right sibling page index
/// - n bytes: header bytes, indicate whether every slot of the page is used or
///   not.
/// - n bytes: tuple bytes
pub struct BTreeLeafPage {
    pid: BTreePageID,

    slot_count: usize,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,
    used_slots: usize,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    // use u32 instead of Option<BTreePageID> to reduce memory
    // footprint
    right_sibling_id: u32,
    left_sibling_id: u32,

    key_field: usize,

    old_data: Vec<u8>,
}

impl BTreeLeafPage {
    fn new(pid: &BTreePageID, bytes: &[u8], schema: &TableSchema) -> Self {
        let mut instance: Self;

        let slot_count = Self::calc_children_cap(&schema);

        let mut reader = Cursor::new(bytes);

        // read page category
        let category = PageCategory::decode(&mut reader, &());
        if category != PageCategory::Leaf {
            panic!(
                "BTreeLeafPage::new: page category is not leaf, category: {:?}",
                category,
            );
        }

        // read left sibling page index
        let left_sibling_id = read_into(&mut reader, &());

        // read right sibling page index
        let right_sibling_id = read_into(&mut reader, &());

        // read header
        let header = BitVec::decode(&mut reader, &());
        let used_slots = header.iter().filter(|&x| x).count();

        // read tuples
        let mut tuples = Vec::new();
        for i in 0..slot_count {
            let tuple: Tuple;
            if !header[i] {
                // skip empty tuple
                tuple = Tuple::new(&Vec::new(), 0);
            } else {
                tuple = Tuple::decode(&mut reader, schema);
            }
            tuples.push(tuple);
        }

        instance = Self {
            pid: pid.clone(),
            slot_count,
            header,
            used_slots,
            tuples,
            right_sibling_id,
            left_sibling_id,
            key_field: schema.get_key_pos(),
            old_data: Vec::new(),
        };

        instance.set_before_image(schema);
        return instance;
    }

    pub fn set_right_pid(&mut self, pid: Option<BTreePageID>) {
        match pid {
            Some(pid) => {
                self.right_sibling_id = pid.page_index;
            }
            None => {
                self.right_sibling_id = EMPTY_PID;
            }
        }
    }

    pub fn get_right_pid(&self) -> Option<BTreePageID> {
        if self.right_sibling_id == EMPTY_PID {
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
                self.left_sibling_id = EMPTY_PID;
            }
        }
    }

    pub fn get_left_pid(&self) -> Option<BTreePageID> {
        if self.left_sibling_id == EMPTY_PID {
            return None;
        } else {
            return Some(BTreePageID::new(
                PageCategory::Leaf,
                self.get_pid().table_id,
                self.left_sibling_id,
            ));
        }
    }

    pub fn get_slots_count(&self) -> usize {
        self.slot_count
    }

    /// stable means at least half of the page is occupied
    pub fn stable(&self) -> bool {
        // TODO: what if this page is the root page?
        todo!();

        let stable_threshold = ceil_div(self.slot_count, 2);
        return self.tuples_count() >= stable_threshold;
    }

    /// Returns the number of empty slots on this page.
    pub fn empty_slots_count(&self) -> usize {
        return self.slot_count - self.used_slots;
    }

    /// Returns the number of tuples currently stored on this page
    pub fn tuples_count(&self) -> usize {
        return self.used_slots;
    }

    /// Adds a tuple to the page such that all tuples remain in sorted order.
    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<(), SmallError> {
        // find the first empty slot
        let mut first_empty_slot: i64 = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i as i64;
                break;
            }
        }

        // Find the last key less than or equal to the key that we are
        // inserting. Then we can insert the new tuple after that key.
        //
        // -1 indicate there is no such key less than tuple.key, so
        // the tuple should be inserted in slot 0 (-1 + 1).
        //
        // TODO: use binary search insead and do the benchmark (the tricky
        // part is that not all slots are used)
        let mut last_less_slot: i64 = -1;
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                if self.tuples[i].get_cell(self.key_field) < tuple.get_cell(self.key_field) {
                    last_less_slot = i as i64;
                } else {
                    break;
                }
            }
        }

        // shift records back or forward to fill empty slot and make
        // room for new record while keeping records in sorted
        // order
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
        self.used_slots += 1;

        return Ok(());
    }

    // Move a tuple from one slot to another slot, destination must be
    // empty
    fn move_tuple(&mut self, from: usize, to: usize) {
        // return if the source slot is empty
        if !self.is_slot_used(from) {
            return;
        }

        self.tuples[to] = self.tuples[from].clone();
        self.mark_slot_status(to, true);
        self.mark_slot_status(from, false);
    }

    pub(crate) fn delete_tuple(&mut self, slot_index: usize) {
        self.mark_slot_status(slot_index, false);
        self.used_slots -= 1;
    }

    pub(crate) fn mvcc_delete_tuple(&mut self, tx: &TransactionID, slot_index: usize) {
        self.tuples[slot_index].set_xmax(tx);
    }

    // Delete all "deleted" tuples that are not visible to the given transaction.
    pub(crate) fn delete_invisible_tuples(&mut self, min_action: &TransactionID) {
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                continue;
            }

            let xmax = &self.tuples[i].get_xmax();
            if *xmax != TransactionID::MAX && xmax < min_action {
                self.delete_tuple(i);
            }
        }
    }

    /// Returns true if associated slot on this page is filled.
    fn is_slot_used(&self, slot_index: usize) -> bool {
        self.header[slot_index]
    }

    // mark the slot as empty/filled.
    fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub(crate) fn check_integrity(
        &self,
        parent_pid: &BTreePageID,
        lower_bound: &Option<Cell>,
        upper_bound: &Option<Cell>,
        check_occupancy: bool,
        depth: usize,
    ) -> SmallResult {
        if self.get_pid().category != PageCategory::Leaf {
            return Err(SmallError::new("page category is not leaf"));
        }

        if &self.get_parent_pid() != parent_pid {
            let err_msg = format!(
                "parent pid incorrect, current page: {:?}, actual parent pid: {:?}, expect parent pid: {:?}",
                self.get_pid(),
                self.get_parent_pid(),
                parent_pid,
            );
            return Err(SmallError::new(&err_msg));
        }

        let mut previous = lower_bound.clone();
        let it = BTreeLeafPageIterator::new(self);
        for tuple in it {
            if let Some(previous) = previous {
                if previous > tuple.get_cell(self.key_field) {
                    let err_msg = format!(
                        "previous: {:?}, current: {:?}, page_id: {:?}",
                        previous,
                        tuple.get_cell(self.key_field),
                        self.get_pid(),
                    );
                    return Err(SmallError::new(&err_msg));
                }
            }
            previous = Some(tuple.get_cell(self.key_field));
        }

        if let Some(upper_bound) = upper_bound {
            if let Some(previous) = previous {
                if &previous > upper_bound {
                    let err_msg = format!(
                        "the last tuple exceeds upper_bound, last tuple: {:?}, upper bound: {:?}",
                        previous, upper_bound,
                    );
                    return Err(SmallError::new(&err_msg));
                }
            }
        }

        if check_occupancy && depth > 0 {
            assert!(self.tuples_count() >= self.get_slots_count() / 2);
        }

        return Ok(());
    }

    pub fn iter(&self) -> BTreeLeafPageIterator {
        BTreeLeafPageIterator::new(self)
    }

    /// Return all slots that satisfy the predicate.
    pub(crate) fn search(&self, predicate: &Predicate) -> Vec<usize> {
        let mut result = Vec::new();
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                let tuple = &self.tuples[i];
                let cell = tuple.get_cell(predicate.field_index);
                if predicate.matches(&cell) {
                    result.push(i);
                }
            }
        }
        return result;
    }
}

/// Methods for accessing const attributes.
impl BTreeLeafPage {
    /// Get the capacity of children (tuples) in this page.
    pub fn calc_children_cap(schema: &TableSchema) -> usize {
        let bits_per_tuple_including_header = schema.get_tuple_size() * 8 + 1;

        // extraBits:
        // - page category (4 bytes)
        // - parent pointer (`INDEX_SIZE` bytes)
        // - left sibling pointer (`INDEX_SIZE` bytes)
        // - right sibling pointer (`INDEX_SIZE` bytes)
        // - header size (2 bytes)
        let extra_bits = (4 + 3 * INDEX_SIZE + 2) * 8;

        (BufferPool::get_page_size() * 8 - extra_bits) / bits_per_tuple_including_header
    }
}

impl BTreePageInit for BTreeLeafPage {
    fn new_empty_page(pid: &BTreePageID, schema: &TableSchema) -> Self {
        let slot_count = Self::calc_children_cap(&schema);

        let parent_pid = BTreePageID::get_root_ptr_pid(pid.get_table_id());

        let mut header = BitVec::new();
        header.grow(slot_count, false);

        // use empty tuples
        let mut tuples = Vec::new();
        for _ in 0..slot_count {
            // use 0 as the tx id for placeholder tuples
            tuples.push(Tuple::new(&Vec::new(), 0));
        }

        Self {
            pid: pid.clone(),
            slot_count,
            header,
            used_slots: 0,
            tuples,
            right_sibling_id: EMPTY_PID,
            left_sibling_id: EMPTY_PID,
            key_field: schema.get_key_pos(),
            old_data: Vec::new(),
        }
    }
}

impl BTreePage for BTreeLeafPage {
    fn new(pid: &BTreePageID, bytes: &[u8], schema: &TableSchema) -> Self {
        Self::new(pid, &bytes, schema)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid.clone()
    }

    /// Generates a byte array representing the contents of this page.
    /// Used to serialize this page to disk.
    ///
    /// The invariant here is that it should be possible to pass the
    /// byte array generated by get_page_data to the BTreeLeafPage
    /// constructor and have it produce an identical BTreeLeafPage
    /// object.
    fn get_page_data(&self, table_schema: &TableSchema) -> Vec<u8> {
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        self.get_pid().category.encode(&mut writer, &());

        // write left sibling page index
        self.left_sibling_id.encode(&mut writer, &());

        // write right sibling page index
        self.right_sibling_id.encode(&mut writer, &());

        // write header
        self.header.encode(&mut writer, &());

        // write tuples
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                self.tuples[i].encode(&mut writer, table_schema);
            } else {
                // write empty bytes
                let mut bytes: Vec<u8> = Vec::new();
                bytes.resize(table_schema.get_tuple_size(), 0);
                writer.write_bytes(&bytes);
            }
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

pub struct BTreeLeafPageIteratorRc {
    page: Arc<RwLock<BTreeLeafPage>>,
    cursor: i64,
    reverse_cursor: i64,

    tx_id: TransactionID,
}

impl BTreeLeafPageIteratorRc {
    pub fn new(tx: &Transaction, page: Arc<RwLock<BTreeLeafPage>>) -> Self {
        let slot_count = page.rl().get_slots_count();
        Self {
            page,
            cursor: -1,
            reverse_cursor: slot_count as i64,

            tx_id: tx.get_id(),
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
                let tuple = page.tuples[cursor].clone();
                if !tuple.visible_to(self.tx_id) {
                    continue;
                }

                return Some(WrappedTuple::new(
                    &page.tuples[cursor].clone(),
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
                    &page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}

pub struct BTreeLeafPageIterator<'page> {
    pub page: &'page BTreeLeafPage,
    cursor: i64,
    reverse_cursor: i64,
}

impl<'page> BTreeLeafPageIterator<'page> {
    pub fn new(page: &'page BTreeLeafPage) -> Self {
        Self {
            page,
            cursor: -1,
            reverse_cursor: page.slot_count as i64,
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
                    &page.tuples[cursor].clone(),
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
                    &page.tuples[cursor].clone(),
                    cursor,
                    page.get_pid(),
                ));
            }
        }
    }
}

impl fmt::Debug for BTreeLeafPage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<leaf page, pid: {:?}, tuple count: {:?}>",
            self.get_pid(),
            self.tuples_count(),
        )
    }
}

impl PageDebug for BTreeLeafPage {}
