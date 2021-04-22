// use crate::btree::buffer_pool::BUFFER_POOL;
use super::database_singleton::singleton_db;
use crate::database::PAGE_SIZE;
use bit_vec::BitVec;
use core::fmt;
use log::debug;
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
    pub key_field: i32,

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
    pub fn new(file_path: &str, key_field: i32, row_scheme: TupleScheme) -> BTreeFile {
        File::create(file_path);

        let f = OpenOptions::new().write(true).open(file_path).unwrap();

        let mut s = DefaultHasher::new();
        file_path.hash(&mut s);

        BTreeFile {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme,
            file: RefCell::new(f),
            table_id: s.finish() as i32,
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    /// May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&self, mut tuple: Tuple) {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let container = self.find_leaf_page(root_pid, tuple.get_field(self.key_field).value);
        if leaf_page.empty_slots_count() == 0 {
            let new_container = self.split_leaf_page(leaf_page, self.key_field);
            let new_leaf_page = (*new_container).borrow_mut();
            new_leaf_page.insert_tuple(tuple);
        } else {
            leaf_page.insert_tuple(tuple);
        }

        // insert the tuple into the leaf page
    }

    /// Split a leaf page to make room for new tuples and
    /// recursively split the parent node as needed to
    /// accommodate a new entry. The new entry should have
    /// a key matching the key field of the first tuple in
    /// the right-hand page (the key is "copied up"), and
    /// child pointers pointing to the two leaf pages
    /// resulting from the split.  Update sibling pointers
    /// and parent pointers as needed.
    ///
    /// Return the leaf page into which a new tuple with
    /// key field "field" should be inserted.
    pub fn split_leaf_page(
        &self,
        mut page: Box<BTreeLeafPage>,
        key_field: i32,
    ) -> Rc<RefCell<Self>> {
        // 1. adding a new page on the right of the existing
        // page and moving half of the tuples to the new page
        let new_page_id = RefCell::new(BTreePageID::new(
            PageCategory::LEAF,
            self.table_id,
            self.get_empty_page_index(),
        ));

        let mut new_page = BTreeLeafPage::new(
            new_page_id,
            BTreeLeafPage::empty_page_data().to_vec(),
            key_field,
            page.tuple_scheme.clone(),
        );

        let tuple_count = page.tuples_count();
        let move_tuple_count = tuple_count / 2;

        let mut it = BTreeLeafPageIterator::new(&page);
        let mut delete_indexes: Vec<usize> = Vec::new();
        for i in 0..move_tuple_count {
            let tuple = it.next().unwrap();
            delete_indexes.push(i);
            new_page.insert_tuple(tuple);
        }
        for i in delete_indexes {
            page.delete_tuple(i);
        }

        // 2. Copy the middle key up into the parent page, and
        // recursively split the parent as needed to accommodate
        // the new entry.

        let parent = self.get_parent_with_empty_slots(page.get_parent_id());

        todo!()
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
    fn get_parent_with_empty_slots(&self, parentId: BTreePageID) -> &mut BTreeInternalPage {
        // create a parent node if necessary
        // this will be the new root of the tree
        if parentId.category == PageCategory::ROOT_POINTER {
            let empty_page_index = self.get_empty_page_index();
            let new_page_id =
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
            let root_pointer_page = singleton_db()
                .get_buffer_pool()
                .get_root_pointer_page(&BTreePageID::new(
                    PageCategory::ROOT_POINTER,
                    self.table_id,
                    0,
                ))
                .unwrap();

            (*root_pointer_page).set_root_id(new_page_id.page_index);

            // match &mut *v {
            //     PageEnum::BTreeRootPointerPage { page } => {
            //         page.set_root_id(new_page_id.page_index);
            //     }
            //     _ => {}
            // }

            // let root_pointer_page = singleton_db()
            //     .get_buffer_pool()
            //     .get_page(&BTreePageID::new(
            //         PageCategory::ROOT_POINTER,
            //         self.table_id,
            //         0,
            //     ))
            //     .unwrap();
            
            // match (&*root_pointer_page).borrow() {
            //     RefCell<PageEnum::BTreeInternalPage{page}> => {}
            //     _ => {}
            // }


            // let mut v = (*root_pointer_page).borrow_mut();
            // match &mut *v {
            //     PageEnum::BTreeInternalPage { page } => {
            //         // return page;
            //     }
            //     _ => {}
            // }
        }

        todo!()
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
    pub fn find_leaf_page(&self, page_id: BTreePageID, _field: i32) -> Rc<Box<BTreeLeafPage>> {
        if page_id.category == PageCategory::LEAF {
            // get page and return directly
            debug!("arrived leaf page");

            // get page from buffer pool
            // let container = singleton_db().get_buffer_pool();
            let db = singleton_db();
            let mut buffer_pool = db.get_buffer_pool();
            let page = buffer_pool.get_leaf_page(&page_id).unwrap();

            return page

            // return page.downcast


            // let v = (*page).borrow_mut();
            // let p = (*page).take();

            // match &*v {
            //     PageEnum::BTreeRootPointerPage { page } => {}
            //     PageEnum::BTreeInternalPage { page } => {}
            //     PageEnum::BTreeLeafPage { page } => {}
            // }
            // let a = v.as_any().downcast_ref::<BTreeLeafPage>().unwrap();
            // return Rc::new(RefCell::new(*a));
        }

        todo!()
    }

    pub fn get_file(&self) -> RefMut<File> {
        self.file.borrow_mut()
    }

    // Get the root pointer page. Create the root pointer page
    // and root page if necessary.
    pub fn get_root_pid(&self) -> BTreePageID {
        // if db file is empty, create root pointer page at first
        if self.get_file().metadata().unwrap().len() == 0 {
            debug!("db file empty, start init");
            let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreeLeafPage::empty_page_data();
            let mut n = self.get_file().write(&empty_root_pointer_data).unwrap();
            debug!("write {} bytes", n);
            n = self.get_file().write(&empty_leaf_data).unwrap();
            debug!("write {} bytes", n);
            // self.file.sync_data();

            let file_length = self.get_file().metadata().unwrap().len();
            debug!("write complete, file length: {}", file_length);
        }

        // get root pointer page
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.get_file().read(&mut data);
        let pid = BTreePageID::new(PageCategory::ROOT_POINTER, self.get_id(), 1);
        let root_pointer_page = BTreeRootPointerPage::new(pid, data.to_vec());

        root_pointer_page.get_root_pid()
    }

    // Create the root pointer page and root page.
    pub fn db_file_init(&self, mut f: File) {
        debug!("db file empty, start init");
        let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
        let empty_leaf_data = BTreeLeafPage::empty_page_data();
        f.write(&empty_root_pointer_data);
        f.write(&empty_leaf_data);
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
    key_field: i32,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    tuple_scheme: TupleScheme,

    parent: i32,

    page_id: RefCell<BTreePageID>,
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
        page_id: RefCell<BTreePageID>,
        bytes: Vec<u8>,
        key_field: i32,
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
            page_id,
        }
    }

    pub fn get_parent_id(&self) -> BTreePageID {
        if self.parent == 0 {
            return BTreePageID::new(
                PageCategory::ROOT_POINTER,
                self.page_id.borrow().table_id,
                0,
            );
        }
        // self.parent

        todo!()
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
    pub fn insert_tuple(&mut self, mut tuple: Tuple) {
        // find the first empty slot
        let mut first_empty_slot = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i;
                debug!("first emply slot: {}", first_empty_slot);
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
        // debug!("tuple on {} slot: {}", first_empty_slot, tuple);
        self.mark_slot_used(first_empty_slot);
        // debug!("header: {:b}", self.header[0]);
        {}
    }

    pub fn delete_tuple(&mut self, slot_index: usize) {
        self.mark_slot_used(slot_index);
    }

    // Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        let bv = BitVec::from_bytes(&self.header);
        bv[slot_index]
    }

    pub fn mark_slot_used(&mut self, slot_index: usize) {
        let mut bv = BitVec::from_bytes(&self.header);
        bv.set(slot_index, true);
        self.header = bv.to_bytes();

        // persistent changes
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

    root_id: usize,
}

impl BTreeRootPointerPage {
    pub fn new(pid: BTreePageID, bytes: Vec<u8>) -> Self {
        let root_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        Self { pid, root_id }
    }

    pub fn page_size() -> usize {
        PAGE_SIZE
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        BTreePageID::new(PageCategory::LEAF, self.pid.table_id, self.root_id)
    }

    pub fn set_root_id(&mut self, id: usize) {
        self.root_id = id;
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

    pub fn get_table_id(&self) -> i32 {
        self.table_id
    }
}

pub struct BTreeInternalPage {}
