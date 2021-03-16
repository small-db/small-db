use crate::database::PAGE_SIZE;
use bit_vec::BitVec;
use log::{debug, info};
use std::{borrow::BorrowMut, cell::Cell, convert::TryInto, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, path::Path, rc::Rc};

use crate::tuple::{Tuple, TupleScheme};

use super::tuple::BTreeTuple;

// B+ Tree
pub struct BTreeFile<'path> {
    // the file that stores the on-disk backing store for this B+ tree
    // file.
    file_path: &'path Path,
    // the field which index is keyed on
    key: i32,
    // the tuple descriptor of tuples in the file
    tuple_scheme: TupleScheme,

    file: File,
}

impl<'path> BTreeFile<'_> {
    pub fn new(file_path: &Path, key: i32, row_scheme: TupleScheme) -> BTreeFile {
        File::create(file_path);

        let mut f = OpenOptions::new().write(true).open(file_path).unwrap();

        BTreeFile {
            file_path,
            key,
            tuple_scheme: row_scheme,
            file: f,
        }
    }

    // Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    // May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&mut self, mut tuple: Tuple) {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let mut leaf_page = self.find_leaf_page(root_pid, tuple.get_field(self.key).value);

        // insert the tuple into the leaf page
        leaf_page.insert_tuple(tuple);
    }

    // Recursive function which finds and locks the leaf page in the B+ tree corresponding to
    // the left-most page possibly containing the key field f. It locks all internal
    // nodes along the path to the leaf node with READ_ONLY permission, and locks the
    // leaf node with permission perm.
    // If f is null, it finds the left-most leaf page -- used for the iterator
    pub fn find_leaf_page(&mut self, page_id: BTreePageID, field: i32) -> BTreeLeafPage {
        if page_id.category == PageCategory::LEAF {
            // get page and return directly
            debug!("arrived leaf page");

            // read page content
            let page_start = (page_id.page_index - 1) * PAGE_SIZE as i32;
            self.file.seek(SeekFrom::Start(page_start as u64));

            let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
            self.file.read(&mut data);

            // instantiate page
            let key_field = 1;
            let page = BTreeLeafPage::new(data.to_vec(), key_field, self.tuple_scheme.copy());

            // return
            return page;
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
        let pid = BTreePageID::new(PageCategory::ROOT_POINTER, 1);
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

    pub fn pages_count(&self) -> i32 {
        let file_len = self.file.metadata().unwrap().len();
        debug!("file length: {}", file_len);
        (file_len / PAGE_SIZE as u64) as i32
    }
}

pub struct BTreeLeafPage {
    slot_count: i32,

    // header bytes
    header: Vec<u8>,

    // which field/column the b+ tree is indexed on
    key_field: i32,

    // all tuples (include empty tuples)
    tuples: Vec<Tuple>,

    tuple_scheme: TupleScheme,
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
            slot_count: slot_count as i32,
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
        let mut less_or_equal_key = -1;
        let key = tuple.get_field(self.key_field);
        for i in 0..self.slot_count {
            if self.is_slot_used(i) {
                if self.tuples[i as usize].get_field(self.key_field) <= key {
                    less_or_equal_key = i;
                } else {
                    break;
                }
            }
        }
        debug!("less_or_equal_key: {}", less_or_equal_key);

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order

        // insert new record into the correct spot in sorted order
        self.tuples[first_empty_slot as usize] = tuple;
        self.mark_slot_used(first_empty_slot);
    }

    // Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: i32) -> bool {
        let mut bv = BitVec::from_bytes(&self.header);
        bv[slot_index as usize]
    }

    pub fn mark_slot_used(&self, slot_index: i32) {
        let mut bv = BitVec::from_bytes(&self.header);
        bv.set(slot_index as usize, true);
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }
}

// Why we need boot BTreeRootPointerPage and BTreeRootPage?
// Because as the tree rebalance (growth, shrinking), location
// of the rootpage will change. So we need the BTreeRootPointerPage,
// which is always placed at the beginning of the database file
// and points to the rootpage. So we can find the location of
// rootpage easily.
pub struct BTreeRootPointerPage {
    root_id: i32,
}

impl BTreeRootPointerPage {
    pub fn new(id: BTreePageID, bytes: Vec<u8>) -> Self {
        let root_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        Self { root_id }
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        BTreePageID::new(PageCategory::LEAF, self.root_id)
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

#[derive(PartialEq, Copy, Clone)]
pub enum PageCategory {
    ROOT_POINTER,
    INTERNAL,
    LEAF,
    HEADER,
}

// PageID identifies a unique page, and contains the
// necessary metadata
// TODO: PageID must be hashable
#[derive(Copy, Clone)]
pub struct BTreePageID {
    // category indicates the category of the page
    pub category: PageCategory,

    // page_index represents the position of the page in
    // the table, start from 0
    pub page_index: i32,
}

impl BTreePageID {
    pub fn new(category: PageCategory, page_index: i32) -> Self {
        Self {
            category,
            page_index,
        }
    }
}
