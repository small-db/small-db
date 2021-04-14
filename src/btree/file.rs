// use crate::btree::buffer_pool::BUFFER_POOL;
use crate::database::PAGE_SIZE;
use bit_vec::BitVec;
use log::{debug, info};
use rand::Rng;
use std::{
    borrow::BorrowMut,
    cell::{Cell, RefCell},
    collections::btree_set::Difference,
    convert::TryInto,
    fs::{File, OpenOptions},
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

use super::{database::Database, tuple::BTreeTuple};

// B+ Tree
pub struct BTreeFile {
    // the file that stores the on-disk backing store for this B+ tree
    // file.
    file_path: String,

    // the field which index is keyed on
    key_field: i32,

    // the tuple descriptor of tuples in the file
    tuple_scheme: TupleScheme,

    file: File,

    // a random int
    table_id: i32,

    db: Weak<Database>,
}

impl<'path> BTreeFile {
    pub fn new(
        file_path: &str,
        key_field: i32,
        row_scheme: TupleScheme,
        db: Weak<Database>,
    ) -> BTreeFile {
        File::create(file_path);

        let mut f = OpenOptions::new().write(true).open(file_path).unwrap();

        let table_id: i32 = rand::thread_rng().gen();

        BTreeFile {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme,
            file: f,
            table_id,

            db: Weak::clone(&db),
        }
    }

    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    /// May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&mut self, mut tuple: Tuple) {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let container = self.find_leaf_page(root_pid, tuple.get_field(self.key_field).value);
        let mut leaf_page = (*container).borrow_mut();
        if leaf_page.empty_slots_count() == 0 {
            let mut new_container = BTreeLeafPage::split_leaf_page(leaf_page, self.key_field);
            let mut new_leaf_page = (*new_container).borrow_mut();
            new_leaf_page.insert_tuple(tuple);
        } else {
            leaf_page.insert_tuple(tuple);
        }

        // insert the tuple into the leaf page
    }

    // Recursive function which finds and locks the leaf page in the B+ tree corresponding to
    // the left-most page possibly containing the key field f. It locks all internal
    // nodes along the path to the leaf node with READ_ONLY permission, and locks the
    // leaf node with permission perm.
    // If f is null, it finds the left-most leaf page -- used for the iterator
    pub fn find_leaf_page(
        &mut self,
        page_id: BTreePageID,
        field: i32,
    ) -> Rc<RefCell<BTreeLeafPage>> {
        if page_id.category == PageCategory::LEAF {
            // get page and return directly
            debug!("arrived leaf page");

            // // read page content
            // let page_start = (page_id.page_index - 1) * PAGE_SIZE as i32;
            // self.file.seek(SeekFrom::Start(page_start as u64));

            // let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
            // self.file.read(&mut data);

            // // instantiate page
            // let key_field = 1;
            // let page = BTreeLeafPage::new(data.to_vec(), key_field, self.tuple_scheme.copy());

            // get page from buffer pool
            // let page = BUFFER_POOL.get(&page_id.page_index);
            let container = self.db.upgrade().unwrap().get_buffer_pool();
            let mut buffer_pool = (*container).borrow_mut();
            let page = buffer_pool.get_page(&page_id).unwrap();

            // return
            return (*page).clone();
        }

        todo!()
    }

    // Get the root pointer page. Create the root pointer page
    // and root page if necessary.
    pub fn get_root_pid(&mut self) -> BTreePageID {
        // if db file is empty, create root pointer page at first
        if self.file.metadata().unwrap().len() == 0 {
            debug!("db file empty, start init");
            let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreeLeafPage::empty_page_data();
            let mut n = self.file.write(&empty_root_pointer_data).unwrap();
            debug!("write {} bytes", n);
            n = self.file.write(&empty_leaf_data).unwrap();
            debug!("write {} bytes", n);
            // self.file.sync_data();

            let file_length = self.file.metadata().unwrap().len();
            debug!("write complete, file length: {}", file_length);
        }

        // get root pointer page
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.file.read(&mut data);
        let pid = BTreePageID::new(PageCategory::ROOT_POINTER, self.table_id, 1);
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
        let file_len = self.file.metadata().unwrap().len() as usize;
        debug!("file length: {}", file_len);
        (file_len - BTreeRootPointerPage::page_size()) / PAGE_SIZE
    }
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
}

pub struct BTreeLeafPageIterator<'a> {
    page: &'a BTreeLeafPage,
    cursor: usize,
}

impl BTreeLeafPage {
    pub fn new(bytes: Vec<u8>, key_field: i32, tuple_scheme: TupleScheme) -> Self {
        let header_size = Self::get_header_size() as usize;
        let slot_count = 100;

        // init tuples
        let mut tuples = Vec::new();
        for i in 0..slot_count {
            let start = header_size + i * tuple_scheme.get_size();
            let end = start + tuple_scheme.get_size();
            let t = Tuple::new(tuple_scheme.copy(), &bytes[start..end]);
            tuples.push(t);
        }

        Self {
            slot_count,
            header: bytes[..header_size].to_vec(),
            key_field,
            tuples,
            tuple_scheme,
        }
    }

    // Retrieve the maximum number of tuples this page can hold.
    pub fn get_max_tuples() -> i32 {
        100
    }

    pub fn empty_slots_count(&self) -> usize {
        let mut count = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                count += 1;
            }
        }
        debug!("empty slot on page: {}", count);
        count
    }

    /// Returns the number of tuples currently stored on this page
    pub fn tuples_count(&self) -> usize {
        self.slot_count - self.empty_slots_count()
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
    pub fn split_leaf_page(mut page: RefMut<Self>, key_field: i32) -> Rc<RefCell<Self>> {
        let mut new_page = BTreeLeafPage::new(
            BTreeLeafPage::empty_page_data().to_vec(),
            key_field,
            page.tuple_scheme.copy(),
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

        todo!()
    }

    // Computes the number of bytes in the header of
    // a page in a BTreeFile with each tuple occupying
    // tupleSize bytes
    pub fn get_header_size() -> i32 {
        100 / 8 + 1
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
        debug!("less_or_equal_key: {}", less_or_equal_key);

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order

        // insert new record into the correct spot in sorted order
        self.tuples[first_empty_slot] = tuple.copy();
        debug!("tuple on {} slot: {}", first_empty_slot, tuple);
        self.mark_slot_used(first_empty_slot);
        debug!("header: {:b}", self.header[0]);
        {}
    }

    pub fn delete_tuple(&mut self, slot_index: usize) {
        self.mark_slot_used(slot_index);
    }

    // Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: usize) -> bool {
        let mut bv = BitVec::from_bytes(&self.header);
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

    root_id: i32,
}

impl BTreeRootPointerPage {
    pub fn new(pid: BTreePageID, bytes: Vec<u8>) -> Self {
        let root_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
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

// PageID identifies a unique page, and contains the
// necessary metadata
// TODO: PageID must be hashable
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    // category indicates the category of the page
    pub category: PageCategory,

    // page_index represents the position of the page in
    // the table, start from 0
    pub page_index: i32,

    pub table_id: i32,
}

impl BTreePageID {
    pub fn new(category: PageCategory, table_id: i32, page_index: i32) -> Self {
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
