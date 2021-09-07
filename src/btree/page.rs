use std::{borrow::Borrow, cell::RefCell, convert::TryInto, fmt, rc::Rc};

use bit_vec::BitVec;
use log::{debug, info};

use crate::field::get_type_length;

use super::buffer_pool::BufferPool;
use super::tuple::{Tuple, TupleScheme};

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
    pid: BTreePageID,

    parent_pid: BTreePageID,
}

impl BTreeBasePage {
    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    pub fn get_parent_pid(&self) -> BTreePageID {
        self.parent_pid
    }

    pub fn set_parent_id(&mut self, id: &BTreePageID) {
        self.parent_pid = id.clone();
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
}

pub struct BTreeLeafPageIterator {
    page: Rc<RefCell<BTreeLeafPage>>,
    cursor: usize,
}

impl BTreeLeafPageIterator {
    pub fn new(page: Rc<RefCell<BTreeLeafPage>>) -> Self {
        Self { page, cursor: 0 }
    }
}

impl Iterator for BTreeLeafPageIterator {
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
        debug!("set root pid: {}", pid);
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

pub struct BTreeInternalPage {
    page: BTreeBasePage,

    keys: Vec<i32>,
    children: Vec<BTreePageID>,

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
        page_id: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self {
        let key_size =
            get_type_length(tuple_scheme.fields[key_field].field_type);
        let slot_count = Self::get_max_entries(key_size) + 1;
        let header_size = Self::get_header_size(slot_count) as usize;

        let mut keys: Vec<i32> = Vec::new();
        let mut children: Vec<BTreePageID> = Vec::new();
        keys.resize(slot_count, 0);
        children.resize(slot_count, BTreePageID::new(PageCategory::Leaf, 0, 0));

        Self {
            page: BTreeBasePage {
                pid: page_id.borrow().clone(),
                parent_pid: BTreePageID::empty(),
            },
            keys,
            children,
            slot_count,
            header: BitVec::from_bytes(&bytes[..header_size]),
            tuple_scheme: tuple_scheme.clone(),
            key_field,
        }
    }

    pub fn dig(&self) {
        info!("page id: {}, parent pid: {}", self.pid, self.parent_pid);
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
        self.pid
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
            self.keys[1] = e.key;
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
                if self.keys[i] < e.key {
                    panic!("key is not in order");
                }
                break;
            }
        }

        if less_or_eq_slot == -1 {
            panic!("no less or equal slot, page id: {}", self.pid);
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
            for i in less_or_eq_slot + 1..empty_slot {
                self.move_entry(i as usize, i as usize + 1);
            }
            good_slot = less_or_eq_slot + 1
        }

        self.keys[good_slot as usize] = e.key;
        self.children[good_slot as usize] = e.get_right_child();
        self.mark_slot_status(good_slot as usize, true);
    }

    fn move_entry(&mut self, from: usize, to: usize) {
        if self.is_slot_used(from) && !self.is_slot_used(to) {
            self.keys[to] = self.keys[from];
            self.children[to] = self.children[from];
            self.mark_slot_status(from, false);
            self.mark_slot_status(to, true);
        }
    }

    fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }
}

/*
All of the entries or tuples in the left child page should be less than or equal to
the key, and all of the entries or tuples in the right child page should be greater
than or equal to the key.
*/
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
            return Some(Entry::new(
                self.page.keys[self.cursor],
                &self.page.children[self.cursor - 1],
                &self.page.children[self.cursor],
            ));
        }
    }
}

pub fn empty_page_data() -> Vec<u8> {
    let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
    data
}
