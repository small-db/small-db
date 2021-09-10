use std::{cell::RefCell, convert::TryInto, fmt, rc::Rc};

use bit_vec::BitVec;
use log::{debug, info};

use crate::field::{get_type_length, IntField};

use super::{
    buffer_pool::BufferPool,
    tuple::{Tuple, TupleScheme},
};

use super::consts::INDEX_SIZE;

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

    let c = PageCategory::Internal;
    println!("{}", c);
    assert_eq!(format!("{}", c), "INTERNAL");
    assert_eq!(format!("{:?}", c), "INTERNAL");
}

pub struct BTreeBasePage {
    pub pid: BTreePageID,

    pub parent_pid: BTreePageID,
}

impl BTreeBasePage {
    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    pub fn get_parent_pid(&self) -> BTreePageID {
        self.parent_pid
    }

    pub fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.parent_pid = pid.clone();
    }

    pub fn empty_page_data() -> Vec<u8> {
        let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
        data
    }
}

pub struct BTreeLeafPage {
    page: BTreeBasePage,

    pub slot_count: usize,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    pub tuple_scheme: TupleScheme,

    right_sibling_id: usize,

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
        page_id: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self {
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
            page: BTreeBasePage {
                pid: page_id.clone(),
                parent_pid: BTreePageID::empty(),
            },
            slot_count,
            header: BitVec::from_bytes(&bytes[..header_size]),
            tuples,
            tuple_scheme: tuple_scheme.clone(),
            right_sibling_id: 0,
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
                self.pid.table_id,
                self.right_sibling_id,
            ));
        }
    }

    /**
    Retrieve the maximum number of tuples this page can hold.
    */
    pub fn get_max_tuples(scheme: &TupleScheme) -> usize {
        let bits_per_tuple_including_header = scheme.get_size() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent
        // pointer
        let index_size: usize = 4;
        let extra_bits = 3 * index_size * 8;
        (BufferPool::get_page_size() * 8 - extra_bits)
            / bits_per_tuple_including_header
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

    pub fn delete_tuple(&mut self, slot_index: &usize) {
        self.mark_slot_status(*slot_index, false);
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
        lower_bound: Option<IntField>,
        upper_bound: Option<IntField>,
        check_occupancy: bool,
        depth: usize,
    ) {
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
            assert!(
                self.tuples_count()
                    >= Self::get_max_tuples(&self.tuple_scheme) / 2
            );
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
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let page = (*self.page).borrow();
        while self.cursor < page.slot_count {
            if page.is_slot_used(self.cursor) {
                let tuple = page.tuples[self.cursor].clone();
                self.cursor += 1;
                return Some(tuple);
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
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.page.slot_count {
            if self.page.is_slot_used(self.cursor) {
                let tuple = self.page.tuples[self.cursor].clone();
                self.cursor += 1;
                return Some(tuple);
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
            cursor: page.slot_count,
        }
    }
}

impl<'page> Iterator for BTreeLeafPageReverseIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.page.is_slot_used(self.cursor) {
                let tuple = self.page.tuples[self.cursor].clone();
                self.cursor -= 1;
                return Some(tuple);
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
    base: BTreeBasePage,

    // The root_pid in mandatory to avoid a bunch of Option & match
    root_pid: BTreePageID,
}

impl std::ops::Deref for BTreeRootPointerPage {
    type Target = BTreeBasePage;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl std::ops::DerefMut for BTreeRootPointerPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl BTreeRootPointerPage {
    pub fn new(pid: &BTreePageID, bytes: Vec<u8>) -> Self {
        let root_page_index =
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: root_page_index,

            // TODO: set table id
            table_id: 0,
        };
        Self {
            base: BTreeBasePage {
                pid: pid.clone(),
                parent_pid: BTreePageID::empty(),
            },

            root_pid,
        }
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

impl fmt::Debug for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl BTreePageID {
    pub fn new(
        category: PageCategory,
        table_id: i32,
        page_index: usize,
    ) -> Self {
        Self {
            category,
            page_index,
            table_id,
        }
    }

    pub fn empty() -> Self {
        Self {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: 0,
        }
    }

    pub fn get_table_id(&self) -> &i32 {
        &self.table_id
    }
}

pub fn empty_page_data() -> Vec<u8> {
    let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
    data
}
