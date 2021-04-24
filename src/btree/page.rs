use std::{borrow::Borrow, cell::RefCell, convert::TryInto, fmt};

use bit_vec::BitVec;

use crate::field::{get_type_length, FieldItem};

use super::tuple::{Tuple, TupleScheme};

use super::consts::INDEX_SIZE;
use super::consts::PAGE_SIZE;

#[derive(PartialEq, Copy, Clone, Eq, Hash)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

impl fmt::Display for PageCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageCategory::RootPointer => {
                write!(f, "ROOT_POINTER")
            }
            PageCategory::Internal => {
                write!(f, "INTERNAL")
            }
            PageCategory::Leaf => {
                write!(f, "LEAF")
            }
            PageCategory::Header => {
                write!(f, "HEADER")
            }
        }
    }
}

impl fmt::Debug for PageCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[test]
fn test_page_category() {
    assert_ne!(PageCategory::Header, PageCategory::Leaf);
    if PageCategory::Leaf == PageCategory::RootPointer {
        println!("error")
    } else {
        println!("ok")
    }
    let c = PageCategory::Header;
    match c {
        PageCategory::Leaf => {
            println!("error")
        }
        PageCategory::Header => {
            println!("ok")
        }
        _ => {}
    }
    println!("{}", c);
    assert_eq!(format!("{}", c), "HEADER");
}

pub struct BTreeLeafPage {
    pub slot_count: usize,

    // header bytes
    header: Vec<u8>,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    pub tuple_scheme: TupleScheme,

    parent: usize,

    pub page_id: BTreePageID,
}

impl BTreeLeafPage {
    pub fn new(page_id: &BTreePageID, bytes: Vec<u8>, tuple_scheme: TupleScheme) -> Self {
        let slot_count = Self::get_max_tuples(&tuple_scheme);
        let header_size = Self::get_header_size(slot_count) as usize;

        // init tuples
        let mut tuples = Vec::new();
        for i in 0..slot_count {
            let start = header_size + i * tuple_scheme.get_size();
            let end = start + tuple_scheme.get_size();
            let t = Tuple::new(tuple_scheme.clone(), &bytes[start..end]);
            tuples.push(t);
        }

        Self {
            slot_count,
            header: bytes[..header_size].to_vec(),
            tuples,
            tuple_scheme,
            parent: 0,
            page_id: *page_id,
        }
    }

    pub fn set_parent_id(&mut self, id: &BTreePageID) {
        self.parent = id.page_index;
    }

    pub fn get_parent_id(&self) -> BTreePageID {
        if self.parent == 0 {
            return BTreePageID::new(PageCategory::RootPointer, self.page_id.borrow().table_id, 0);
        }

        return BTreePageID::new(
            PageCategory::Internal,
            self.page_id.borrow().table_id,
            self.parent,
        );
    }

    /**
    Retrieve the maximum number of tuples this page can hold.
    */
    pub fn get_max_tuples(scheme: &TupleScheme) -> usize {
        let bits_per_tuple_including_header = scheme.get_size() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        let index_size: usize = 4;
        let extra_bits = 3 * index_size * 8;
        // (BufferPool.getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader; //round down
        // singleton_db().get_buffer_pool()
        (PAGE_SIZE * 8 - extra_bits) / bits_per_tuple_including_header
        // todo!()
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
    pub fn get_header_size(slot_count: usize) -> usize {
        slot_count / 8 + 1
    }

    // Adds the specified tuple to the page such that all records remain in sorted order;
    // the tuple should be updated to reflect
    // that it is now stored on this page.
    // tuple: The tuple to add.
    pub fn insert_tuple(&mut self, tuple: &Tuple) {
        // find the first empty slot
        let mut first_empty_slot = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i;
                // debug!("first emply slot: {}", first_empty_slot);
                break;
            }
        }

        // find the last key less than or equal to the key being inserted

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order

        // insert new record into the correct spot in sorted order
        self.tuples[first_empty_slot] = tuple.clone();
        self.mark_slot_status(first_empty_slot, true);
    }

    pub fn delete_tuple(&mut self, slot_index: &usize) {
        self.mark_slot_status(*slot_index, false);
    }

    /**
    Returns true if associated slot on this page is filled.
    */
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        let bv = BitVec::from_bytes(&self.header);
        bv[slot_index]
    }

    pub fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        let mut bv = BitVec::from_bytes(&self.header);
        bv.set(slot_index, used);
        self.header = bv.to_bytes();
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }
}

pub struct BTreeLeafPageIterator<'a> {
    page: &'a BTreeLeafPage,
    cursor: usize,
}

impl<'a> BTreeLeafPageIterator<'a> {
    pub fn new(page: &'a BTreeLeafPage) -> Self {
        Self { page, cursor: 0 }
    }
}

impl<'a> Iterator for BTreeLeafPageIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.page.slot_count {
            if self.page.is_slot_used(self.cursor) {
                return Some(self.page.tuples[self.cursor].clone());
            } else {
                self.cursor += 1;
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
            cursor: page.slot_count - 1,
        }
    }
}

impl<'page> Iterator for BTreeLeafPageReverseIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.page.is_slot_used(self.cursor) {
                return Some(self.page.tuples[self.cursor].clone());
            } else if self.cursor == 0 {
                return None;
            } else {
                self.cursor -= 1;
            }
        }
    }
}

// Why we need boot BTreeRootPointerPage and BTreeRootPage?
// Because as the tree rebalance (growth, shrinking), location
// of the rootpage will change. So we need the BTreeRootPointerPage,
// which is always placed at the beginning of the database file
// and points to the rootpage. So we can find the location of
// rootpage easily.
pub struct BTreeRootPointerPage {
    root_pid: BTreePageID,
}

impl BTreeRootPointerPage {
    pub fn new(bytes: Vec<u8>) -> Self {
        let root_page_index = i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: root_page_index,

            // TODO: set table id
            table_id: 0,
        };
        Self { root_pid }
    }

    pub fn page_size() -> usize {
        PAGE_SIZE
    }

    /**
    get empty data, init root pid to 1
    */
    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        let mut data = [0; PAGE_SIZE];
        let bytes = 1_i32.to_le_bytes();
        for i in 0..4 {
            data[i] = bytes[i];
        }
        data
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }
}

// PageID identifies a unique page, and contains the
// necessary metadata
// TODO: PageID must be hashable
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    // category indicates the category of the page
    pub category: PageCategory,

    // page_index represents the position of the page in
    // the table, start from 0
    pub page_index: usize,

    pub table_id: i32,
}

impl fmt::Display for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<BTreePageID, catagory: {}, page_index: {}, table_id: {}>",
            self.category, self.page_index, self.table_id,
        )
    }
}

impl BTreePageID {
    pub fn new(category: PageCategory, table_id: i32, page_index: usize) -> Self {
        Self {
            category,
            page_index,
            table_id,
        }
    }

    pub fn get_table_id(&self) -> &i32 {
        &self.table_id
    }
}

pub struct BTreeInternalPage {
    page_id: BTreePageID,

    entries: Vec<Entry>,

    slot_count: usize,

    // header bytes
    header: Vec<u8>,
}

impl BTreeInternalPage {
    pub fn new(page_id: RefCell<BTreePageID>, bytes: Vec<u8>, key_field: &FieldItem) -> Self {
        let slot_count = Self::get_max_entries(get_type_length(key_field.field_type));
        let header_size = Self::get_header_size(slot_count) as usize;

        Self {
            page_id: page_id.borrow().clone(),
            entries: Vec::new(),
            slot_count,
            header: bytes[..header_size].to_vec(),
        }
    }

    fn get_header_size(max_entries_count: usize) -> usize {
        let slots_per_page = max_entries_count + 1;
        let header_bytes = slots_per_page / 8;
        header_bytes
    }

    /**
    Retrieve the maximum number of entries this page can hold. (The number of keys)
    */
    fn get_max_entries(key_size: usize) -> usize {
        let bits_per_entry_including_header = key_size * 8 + INDEX_SIZE * 8 + 1;
        /*
        extraBits are: one parent pointer, 1 byte for child page category,
        one extra child pointer (node with m entries has m+1 pointers to
        children),
        1 bit for extra header (why?)
        */
        let extra_bits = 2 * INDEX_SIZE * 8 + 8;
        let entries_per_page = (PAGE_SIZE * 8 - extra_bits) / bits_per_entry_including_header; //round down
        entries_per_page
    }

    pub fn get_id(&self) -> BTreePageID {
        self.page_id
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

    /**
    Returns true if associated slot on this page is filled.
    */
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        let bv = BitVec::from_bytes(&self.header);
        bv[slot_index]
    }

    /**
    TODO: insert in sorted order
    */
    pub fn insert_entry(&mut self, e: &Entry) {
        self.entries.push(*e)
    }

    pub fn get_entries(&self) -> Vec<Entry> {
        self.entries.to_vec()
    }

    pub fn get_last_entry(&self) -> Entry {
        *self.entries.last().unwrap()
    }
}

#[derive(Clone, Copy)]
pub struct Entry {
    pub key: i32,
    left: BTreePageID,
    right: BTreePageID,
}

impl Entry {
    pub fn new(key: i32, left: &BTreePageID, right: &BTreePageID) -> Self {
        Self {
            key,
            left: *left,
            right: *right,
        }
    }

    pub fn get_left_child(&self) -> BTreePageID {
        self.left
    }

    pub fn get_right_child(&self) -> BTreePageID {
        self.right
    }
}
