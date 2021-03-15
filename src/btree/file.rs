use crate::database::PAGE_SIZE;
use bit_vec::BitVec;
use log::{debug, info};
use std::{
    convert::TryInto,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    rc::Rc,
};

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
    row_scheme: TupleScheme,

    file: File,
}

impl<'path> BTreeFile<'_> {
    pub fn new(file_path: &Path, key: i32, row_scheme: TupleScheme) -> BTreeFile {
        File::create(file_path);

        let mut f = File::open(file_path).unwrap();

        BTreeFile {
            file_path,
            key,
            row_scheme,
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
        let leaf_page = self.find_leaf_page(root_pid, tuple.get_cell(self.key).value);

        // insert the tuple into the leaf page
        leaf_page.insert_tuple(tuple);
    }

    // Recursive function which finds and locks the leaf page in the B+ tree corresponding to
    // the left-most page possibly containing the key field f. It locks all internal
    // nodes along the path to the leaf node with READ_ONLY permission, and locks the
    // leaf node with permission perm.
    // If f is null, it finds the left-most leaf page -- used for the iterator
    pub fn find_leaf_page(&mut self, page_id: BTreePageID, field: i32) -> Rc<BTreeLeafPage> {
        if page_id.category == PageCategory::LEAF {
            // get page and return directly
            debug!("arrived leaf page");

            // read page content
            let page_start = (page_id.page_index - 1) * PAGE_SIZE as i32;
            self.file.seek(SeekFrom::Start(page_start as u64));

            let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
            self.file.read(&mut data);

            // instantiate page
            let page = BTreeLeafPage::new(data.to_vec());

            // return
            return Rc::new(page);
        }

        todo!()
    }

    // Get the root pointer page. Create the root pointer page
    // and root page if necessary.
    pub fn get_root_pid(&self) -> BTreePageID {
        let mut f = File::open(self.file_path).unwrap();

        // if db file is empty, create root pointer page at first
        if f.metadata().unwrap().len() == 0 {
            debug!("db file empty, start init");
            let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreeLeafPage::empty_page_data();
            f.write(&empty_root_pointer_data);
            f.write(&empty_leaf_data);
        }

        // get root pointer page
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        f.read(&mut data);
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

    pub fn num_pages(&self) -> i32 {
        todo!()
    }
}

pub struct BTreeLeafPage {
    slot_count: i32,
    header: Vec<u8>,
}

impl BTreeLeafPage {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            slot_count: 100,
            header: Vec::new(),
        }
    }

    // Adds the specified tuple to the page such that all records remain in sorted order;
    // the tuple should be updated to reflect
    // that it is now stored on this page.
    // tuple: The tuple to add.
    pub fn insert_tuple(&self, tuple: Tuple) {
        // find the first empty slot
        let mut first_empty_slot = 0;
        for i in 0..self.slot_count {
            if !self.is_slot_used(i) {
                first_empty_slot = i;
                break;
            }
        }

        // find the last key less than or equal to the key being inserted

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order

        // insert new record into the correct spot in sorted order

        todo!()
    }

    // Returns true if associated slot on this page is filled.
    pub fn is_slot_used(&self, slot_index: i32) -> bool {
        let mut bv = BitVec::from_bytes(&self.header);
        bv[slot_index as usize]
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
