use crate::btree::page::PageCategory;
use super::{buffer_pool::BufferPool, page::{BTreeLeafPage, BTreeLeafPageIterator, BTreePageID, Entry}};
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
pub struct BTreeTable {
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

impl fmt::Display for BTreeTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<BTreeFile, file: {}, id: {}>",
            self.file_path, self.table_id
        )
    }
}

impl<'path> BTreeTable {
    pub fn new(file_path: &str, key_field: usize, row_scheme: TupleScheme) -> BTreeTable {
        File::create(file_path);

        let f = RefCell::new(OpenOptions::new().write(true).open(file_path).unwrap());

        let mut s = DefaultHasher::new();
        file_path.hash(&mut s);
        let table_id = s.finish() as i32;

        Self::file_init(f.borrow_mut(), table_id);

        BTreeTable {
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
            PageCategory::Leaf,
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
            PageCategory::RootPointer => {
                let empty_page_index = self.get_empty_page_index();
                let new_parent_id =
                    BTreePageID::new(PageCategory::Internal, self.table_id, empty_page_index);

                // write empty page to disk
                let start_pos = BTreeRootPointerPage::page_size() + empty_page_index * PAGE_SIZE;
                match self.get_file().seek(SeekFrom::Start(start_pos as u64)) {
                    Ok(_) => (),
                    Err(_) => (),
                }
                self.get_file().write(&BTreeLeafPage::empty_page_data());
                self.get_file().flush();

                // update the root pointer
                let page_id = BTreePageID::new(PageCategory::RootPointer, self.table_id, 0);
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
            PageCategory::Leaf => {
                // get page and return directly
                return BufferPool::global().get_leaf_page(&page_id).unwrap();
            }
            PageCategory::Internal => {
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
                BTreePageID::new(PageCategory::RootPointer, table_id, 0),
                n
            );
            n = file.write(&empty_leaf_data).unwrap();
            debug!(
                "write page to disk, pid: {}, len: {}",
                BTreePageID::new(PageCategory::Leaf, table_id, 1),
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
            category: PageCategory::RootPointer,
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