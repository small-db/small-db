use crate::database::PAGE_SIZE;
use log::{debug, info};
use std::{fs::File, io::Write, path::Path, rc::Rc};

use super::page_id::PageCategory;
use crate::tuple::{Tuple, TupleScheme};

use super::{page_id::BTreePageID, tuple::BTreeTuple};

// B+ Tree
pub struct BTreeFile<'path> {
    // the file that stores the on-disk backing store for this B+ tree
    // file.
    file_path: &'path Path,
    // the field which index is keyed on
    key: i32,
    // the tuple descriptor of tuples in the file
    row_scheme: TupleScheme,
}

impl<'path> BTreeFile<'_> {
    pub fn new(file_path: &Path, key: i32, row_scheme: TupleScheme) -> BTreeFile {
        BTreeFile {
            file_path,
            key,
            row_scheme,
        }
    }

    // Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    // May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&self, mut tuple: Tuple) {
        // get a read lock on the root pointer page and
        // use it to locate the root page
        // self.read_root_page();
        let root_pid = self.get_root_page().page_id;

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
    pub fn find_leaf_page(&self, page_id: BTreePageID, field: i32) -> Rc<BTreeLeafPage> {
        if page_id.category == PageCategory::LEAF {}

        todo!()
    }

    // Get the root page. Create the root page and root page
    // if necessary.
    pub fn get_root_page(&self) -> Rc<BTreeRootPage> {
        let mut f = File::open(self.file_path).unwrap();

        // if db file is empty, create root ptr page at first
        if f.metadata().unwrap().len() == 0 {
            debug!("file empty");
            let empty_root_pointer_data = BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreeLeafPage::empty_page_data();
            f.write(&empty_root_pointer_data);
            f.write(&empty_leaf_data);
        }

        // get root ptr page

        // get root page

        todo!()
    }

    pub fn num_pages(&self) -> i32 {
        todo!()
    }
}

pub struct BTreeLeafPage {}

impl BTreeLeafPage {
    pub fn insert_tuple(&self, tuple: Tuple) {
        todo!()
    }

    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
    }
}

// Why we need boot BTreeRootPtrPage and BTreeRootPage?
// Because as the tree rebalance (growth, shrinking), location
// of the rootpage will change. So we need the BTreeRootPtrPage,
// which is always placed at the beginning of the database file
// and points to the rootpage. So we can find the location of
// rootpage easily.
pub struct BTreeRootPointerPage {}

impl BTreeRootPointerPage {
    pub fn empty_page_data() -> [u8; PAGE_SIZE] {
        [0; PAGE_SIZE]
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
