// use crate::btree::buffer_pool::BUFFER_POOL;
// use super::database_singleton::singleton_db;
use super::buffer_pool::BufferPool;
use super::buffer_pool::PAGE_SIZE;
use bit_vec::BitVec;
use core::fmt;
use log::{debug, info};
use std::{any::Any, borrow::Borrow};

use std::{
    borrow::BorrowMut,
    cell::{Cell, RefCell},
    collections::{btree_set::Difference, hash_map::DefaultHasher},
    convert::TryInto,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    rc::Rc,
    usize,
};
use std::{
    cell::{Ref, RefMut},
    rc::Weak,
};

use crate::tuple::{Tuple, TupleScheme};

// B+ Tree
pub struct BTreeFile {
    // the file that stores the on-disk backing store for this B+ tree
    // file.
    file_path: String,

    // the field which index is keyed on
    pub key_field: usize,

    // the tuple descriptor of tuples in the file
    pub tuple_scheme: TupleScheme,

    file: RefCell<File>,

    table_id: i32,
}

impl fmt::Display for BTreeFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<BTreeFile, file: {}, id: {}>",
            self.file_path, self.table_id
        )
    }
}

impl<'path> BTreeFile {
    pub fn new(file_path: &str, key_field: usize, row_scheme: TupleScheme) -> BTreeFile {
        File::create(file_path);

        let f = RefCell::new(OpenOptions::new().write(true).open(file_path).unwrap());

        let mut s = DefaultHasher::new();
        file_path.hash(&mut s);
        let table_id = s.finish() as i32;

        Self::file_init(f.borrow_mut(), table_id);

        BTreeFile {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme,
            file: f,
            table_id,
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    /// May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&self, tuple: Tuple) {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let container = self.find_leaf_page(root_pid, tuple.get_field(self.key_field).value);
        let mut leaf_page = (*container).borrow_mut();
        if leaf_page.empty_slots_count() == 0 {
            info!(
                "page full: {}, empty slots: {}",
                leaf_page.page_id.borrow(),
                leaf_page.empty_slots_count()
            );
            info!("page split");
            let new_container = self.split_leaf_page(leaf_page, self.key_field);
            let mut new_leaf_page = (*new_container).borrow_mut();
            new_leaf_page.insert_tuple(&tuple);
        } else {
            leaf_page.insert_tuple(&tuple);
        }

        // insert the tuple into the leaf page
    }

    /**
    Split a leaf page to make room for new tuples and
    recursively split the parent node as needed to
    accommodate a new entry. The new entry should have
    a key matching the key field of the first tuple in
    the right-hand page (the key is "copied up"), and
    child pointers pointing to the two leaf pages
    resulting from the split.  Update sibling pointers
    and parent pointers as needed.

    Return the leaf page into which a new tuple with
    key field "field" should be inserted.
    */
    pub fn split_leaf_page(
        &self,
        mut page: RefMut<BTreeLeafPage>,
        key_field: usize,
    ) -> Rc<RefCell<BTreeLeafPage>> {
        // 1. adding a new page on the right of the existing
        // page and moving half of the tuples to the new page
        let new_page_id = RefCell::new(BTreePageID::new(
            PageCategory::LEAF,
            self.table_id,
            self.get_empty_page_index(),
        ));

        let mut new_page = BTreeLeafPage::new(
            &new_page_id.borrow(),
            BTreeLeafPage::empty_page_data().to_vec(),
            key_field,
            page.tuple_scheme.clone(),
        );

        let tuple_count = page.tuples_count();
        let move_tuple_count = tuple_count / 2;

        let mut it = BTreeLeafPageIterator::new(&page);
        let mut delete_indexes: Vec<usize> = Vec::new();
        let mut key = 0;
        for i in 0..move_tuple_count {
            let tuple = it.next().unwrap();
            delete_indexes.push(i);
            new_page.insert_tuple(&tuple);

            // get key
            if i == move_tuple_count - 1 {
                key = tuple.get_field(key_field).value;
            }
        }
        for i in &delete_indexes {
            page.delete_tuple(i);
        }

        if page.empty_slots_count() != delete_indexes.len() {
            panic!("{}", page.empty_slots_count());
        }

        // 2. Copy the middle key up into the parent page, and
        // recursively split the parent as needed to accommodate
        // the new entry.
        let parent_ref = self.get_parent_with_empty_slots(page.get_parent_id());
        let mut parent = (*parent_ref).borrow_mut();

        let entry = Entry::new(key, &new_page_id.borrow().clone(), &page.page_id.borrow());
        parent.insert_entry(&entry);

        // set parent id
        page.set_parent_id(&parent.get_id());
        new_page.set_parent_id(&parent.get_id());

        let v = BufferPool::global().get_leaf_page(&*page.page_id.borrow());

        v.unwrap()
    }

    fn get_empty_page_index(&self) -> usize {
        self.pages_count() + 1
    }

    /**
    Method to encapsulate the process of getting a parent page
    ready to accept new entries.
    This may mean creating a page to become the new root of
    the tree, splitting the existing parent page if there are
    no empty slots, or simply locking and returning the existing
    parent page.
    */
    fn get_parent_with_empty_slots(
        &self,
        parent_id: BTreePageID,
    ) -> Rc<RefCell<BTreeInternalPage>> {
        // create a parent node if necessary
        // this will be the new root of the tree
        match parent_id.category {
            PageCategory::ROOT_POINTER => {
                let empty_page_index = self.get_empty_page_index();
                let new_parent_id =
                    BTreePageID::new(PageCategory::INTERNAL, self.table_id, empty_page_index);

                // write empty page to disk
                let start_pos = BTreeRootPointerPage::page_size() + empty_page_index * PAGE_SIZE;
                match self.get_file().seek(SeekFrom::Start(start_pos as u64)) {
                    Ok(_) => (),
                    Err(_) => (),
                }
                self.get_file().write(&BTreeLeafPage::empty_page_data());
                self.get_file().flush();

                // update the root pointer
                let page_id = BTreePageID::new(PageCategory::ROOT_POINTER, self.table_id, 0);
                let root_pointer_page = BufferPool::global()
                    .get_root_pointer_page(&page_id)
                    .unwrap();

                (*root_pointer_page)
                    .borrow_mut()
                    .set_root_pid(&new_parent_id);

                let v = BufferPool::global().get_internal_page(&new_parent_id);
                return v.unwrap();
            }
            _ => {
                todo!()
            }
        }
    }

    /**
    Recursive function which finds and locks the leaf page in
    the B+ tree corresponding to the left-most page possibly
    containing the key field f. It locks all internal nodes
    along the path to the leaf node with READ_ONLY permission,
    and locks the leaf node with permission perm.

    If f is null, it finds the left-most leaf page -- used
    for the iterator
    */
    pub fn find_leaf_page(&self, page_id: BTreePageID, field: i32) -> Rc<RefCell<BTreeLeafPage>> {
        match page_id.category {
            PageCategory::LEAF => {
                // get page and return directly
                return BufferPool::global().get_leaf_page(&page_id).unwrap();
            }
            PageCategory::INTERNAL => {
                let page_ref = BufferPool::global().get_internal_page(&page_id).unwrap();
                let page = (*page_ref).borrow();

                for entry in page.get_entries() {
                    if entry.key >= field {
                        let left = entry.get_left_child();
                        return BufferPool::global().get_leaf_page(&left).unwrap();
                    }
                }

                // return right of last entry
                let last_entry = page.get_last_entry();
                let right = last_entry.get_right_child();
                return BufferPool::global().get_leaf_page(&right).unwrap();
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn get_file(&self) -> RefMut<File> {
        self.file.borrow_mut()
    }

    /**
    init file in necessary
    */
    fn file_init(mut file: RefMut<File>, table_id: i32) {
        if file.metadata().unwrap().len() == 0 {
            // if db file is empty, create root pointer page at first
            debug!("db file empty, start init");
            let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreeLeafPage::empty_page_data();
            let mut n = file.write(&empty_root_pointer_data).unwrap();
            debug!(
                "write page to disk, pid: {}, len: {}",
                BTreePageID::new(PageCategory::ROOT_POINTER, table_id, 0),
                n
            );
            n = file.write(&empty_leaf_data).unwrap();
            debug!(
                "write page to disk, pid: {}, len: {}",
                BTreePageID::new(PageCategory::LEAF, table_id, 1),
                n
            );

            let file_length = file.metadata().unwrap().len();
            debug!("write complete, file length: {}", file_length);
        }
    }

    /**
    Get the root page pid. Create the root pointer page
    and root page if necessary.
    */
    pub fn get_root_pid(&self) -> BTreePageID {
        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::ROOT_POINTER,
            page_index: 0,
            table_id: self.table_id,
        };
        let page_ref = BufferPool::global()
            .get_root_pointer_page(&root_pointer_pid)
            .unwrap();
        let page = (*page_ref).borrow();
        let mut root_pid = page.get_root_pid();
        root_pid.table_id = self.get_id();
        root_pid
    }

    /// The count of pages in this BTreeFile
    ///
    /// (BTreeRootPointerPage is not included)
    pub fn pages_count(&self) -> usize {
        let file_len = self.get_file().metadata().unwrap().len() as usize;
        (file_len - BTreeRootPointerPage::page_size()) / PAGE_SIZE
    }
}

pub trait BTreePage {
    fn as_any(&self) -> &dyn Any;
}

pub enum PageEnum {
    BTreeRootPointerPage { page: BTreeRootPointerPage },
    BTreeInternalPage { page: BTreeInternalPage },
    BTreeLeafPage { page: BTreeLeafPage },
}

pub struct BTreeLeafPage {
    slot_count: usize,

    // header bytes
    header: Vec<u8>,

    // which field/column the b+ tree is indexed on
    key_field: usize,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    tuple_scheme: TupleScheme,

    parent: usize,

    pub page_id: BTreePageID,
}

impl BTreePage for BTreeLeafPage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct BTreeLeafPageIterator<'a> {
    page: &'a BTreeLeafPage,
    cursor: usize,
}

impl BTreeLeafPage {
    pub fn new(
        page_id: &BTreePageID,
        bytes: Vec<u8>,
        key_field: usize,
        tuple_scheme: TupleScheme,
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
            slot_count,
            header: bytes[..header_size].to_vec(),
            key_field,
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
            return BTreePageID::new(
                PageCategory::ROOT_POINTER,
                self.page_id.borrow().table_id,
                0,
            );
        }

        return BTreePageID::new(
            PageCategory::INTERNAL,
            self.page_id.borrow().table_id,
            self.parent,
        );
    }

    // Retrieve the maximum number of tuples this page can hold.
    pub fn get_max_tuples(scheme: &TupleScheme) -> usize {
        // 100
        // int bitsPerTupleIncludingHeader = td.getSize() * 8 + 1;
        // // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        // int extraBits = 3 * INDEX_SIZE * 8;
        // int tuplesPerPage = (BufferPool.getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader; //round down
        // return tuplesPerPage;

        let bits_per_tuple_including_header = scheme.get_size() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        let INDEX_SIZE: usize = 4;
        let extra_bits = 3 * INDEX_SIZE * 8;
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
        let mut less_or_equal_key: i32 = -1;
        let key = tuple.get_field(self.key_field);
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                if self.tuples[i as usize].get_field(self.key_field) <= key {
                    less_or_equal_key = i as i32;
                } else {
                    break;
                }
            }
        }
        // debug!("less_or_equal_key: {}", less_or_equal_key);

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order

        // insert new record into the correct spot in sorted order
        self.tuples[first_empty_slot] = tuple.copy();
        self.mark_slot_status(first_empty_slot, true);
    }

    pub fn delete_tuple(&mut self, slot_index: &usize) {
        self.mark_slot_status(*slot_index, false);
    }

    // Returns true if associated slot on this page is filled.
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
                return Some(self.page.tuples[self.cursor].copy());
            } else {
                self.cursor += 1;
            }
        }

        None
    }
}

// Why we need boot BTreeRootPointerPage and BTreeRootPage?
// Because as the tree rebalance (growth, shrinking), location
// of the rootpage will change. So we need the BTreeRootPointerPage,
// which is always placed at the beginning of the database file
// and points to the rootpage. So we can find the location of
// rootpage easily.
pub struct BTreeRootPointerPage {
    pid: BTreePageID,

    root_pid: BTreePageID,
}

impl BTreeRootPointerPage {
    pub fn new(pid: BTreePageID, bytes: Vec<u8>) -> Self {
        let root_page_index = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let root_pid = BTreePageID {
            category: PageCategory::LEAF,
            page_index: root_page_index,

            // TODO: set table id
            table_id: 0,
        };
        Self { pid, root_pid }
    }

    pub fn page_size() -> usize {
        PAGE_SIZE
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }
}

pub struct BTreeRootPage {
    page_id: BTreePageID,
}

impl BTreeRootPage {
    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        todo!()
    }
}

#[derive(PartialEq, Copy, Clone, Eq, Hash)]
// #[derive(Copy, Clone, , Hash)]
pub enum PageCategory {
    ROOT_POINTER,
    INTERNAL,
    LEAF,
    HEADER,
}

impl fmt::Display for PageCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageCategory::ROOT_POINTER => {
                write!(f, "ROOT_POINTER")
            }
            PageCategory::INTERNAL => {
                write!(f, "INTERNAL")
            }
            PageCategory::LEAF => {
                write!(f, "LEAF")
            }
            PageCategory::HEADER => {
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
    assert_ne!(PageCategory::HEADER, PageCategory::LEAF);
    if PageCategory::LEAF == PageCategory::ROOT_POINTER {
        println!("error")
    } else {
        println!("ok")
    }
    let c = PageCategory::HEADER;
    match c {
        PageCategory::LEAF => {
            println!("error")
        }
        PageCategory::HEADER => {
            println!("ok")
        }
        _ => {}
    }
    println!("{}", c);
    assert_eq!(format!("{}", c), "HEADER");
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
}

impl BTreeInternalPage {
    pub fn new(page_id: RefCell<BTreePageID>, _bytes: Vec<u8>, _key_field: usize) -> Self {
        Self {
            page_id: page_id.borrow().clone(),
            entries: Vec::new(),
        }
    }

    pub fn get_id(&self) -> BTreePageID {
        self.page_id
    }

    pub fn insert_entry(&mut self, e: &Entry) {
        // TODO: insert in sorted order

        // self.entries.insert(0, element)
        self.entries.push(*e)
    }

    pub fn get_entries(&self) -> Vec<Entry> {
        self.entries.to_vec()
    }

    pub fn get_last_entry(&self) -> Entry {
        *self.entries.last().unwrap()
    }
}

pub struct BTreeInternalPageIterator<'a> {
    page: &'a BTreeInternalPage,
    cursor: usize,
}

impl<'a> BTreeInternalPageIterator<'a> {
    pub fn new(page: &'a BTreeInternalPage) -> Self {
        Self { page, cursor: 0 }
    }
}

impl<'a> Iterator for BTreeInternalPageIterator<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // while self.cursor < self.page.entry_count {
        //     if self.page.is_slot_used(self.cursor) {
        //         return Some(self.page.tuples[self.cursor].copy());
        //     } else {
        //         self.cursor += 1;
        //     }
        // }

        None
    }
}

#[derive(Clone, Copy)]
pub struct Entry {
    key: i32,
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
