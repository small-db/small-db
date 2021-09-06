use super::{
    buffer_pool::BufferPool,
    page::{
        BTreeInternalPageIterator, BTreeInternalPageReverseIterator,
        BTreeLeafPage, BTreeLeafPageIterator, BTreeLeafPageReverseIterator,
        BTreePageID, BTreeRootPointerPage, Entry,
    },
};
use crate::{
    btree::{
        consts::WRITE_DISK,
        page::{BTreePage, PageCategory},
    },
    field::IntField,
};

use super::consts::PAGE_SIZE;
use core::fmt;
use log::{debug, info};
use std::{borrow::Borrow, cell::Cell, iter::empty, str};

use std::{
    cell::RefCell,
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Seek, SeekFrom, Write},
    rc::Rc,
    usize,
};

use std::cell::RefMut;

use super::{
    page::BTreeInternalPage,
    tuple::{Tuple, TupleScheme},
};

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

    page_index: Cell<usize>,

    split_strategy: Cell<SplitStrategy>,
}

#[derive(Copy, Clone)]
pub enum SplitStrategy {
    MoveHalfToLeft,
    MoveHalfToRight,
    AddLeftWithoutMove,
    AddRightWithoutMove,
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

impl BTreeTable {
    pub fn new(
        file_path: &str,
        key_field: usize,
        row_scheme: TupleScheme,
    ) -> Self {
        File::create(file_path).expect("io error");

        let f = RefCell::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .open(file_path)
                .unwrap(),
        );

        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        let table_id = hasher.finish() as i32;

        Self::file_init(f.borrow_mut(), table_id);

        Self {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme,
            file: f,
            table_id,

            // TODO: init it according to actual condition
            page_index: Cell::new(1),

            split_strategy: Cell::new(SplitStrategy::MoveHalfToRight),
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    pub fn set_split_strategy(&self, strategy: SplitStrategy) {
        self.split_strategy.set(strategy);
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
        let field = tuple.get_field(self.key_field);
        let mut leaf_rc = self.find_leaf_page(root_pid, Some(field));

        if (*leaf_rc).borrow().empty_slots_count() == 0 {
            leaf_rc =
                self.split_leaf_page(leaf_rc, tuple.get_field(self.key_field));
        }
        (*leaf_rc).borrow_mut().insert_tuple(&tuple);
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

    UPDATE:
    split leaf page based on the split strategy.

    # Arguments
    * `field`: the key field of the tuple to be inserted after the split is complete. Necessary to know which of the two pages to return.
    */
    pub fn split_leaf_page(
        &self,
        page_rc: Rc<RefCell<BTreeLeafPage>>,
        field: IntField,
    ) -> Rc<RefCell<BTreeLeafPage>> {
        let new_sibling_rc = self.get_empty_leaf_page();
        let parent_pid: BTreePageID;
        let mut key: i32 = 0;

        // borrow of new_sibling_rc start here
        // borrow of page_rc start here
        {
            let mut new_sibling = (*new_sibling_rc).borrow_mut();
            let mut page = (*page_rc).borrow_mut();

            match self.split_strategy.get() {
                SplitStrategy::MoveHalfToLeft => todo!(),
                SplitStrategy::MoveHalfToRight => {
                    // 1. adding a new page on the right of the existing
                    // page and moving half of the tuples to the new page
                    let tuple_count = page.tuples_count();
                    let move_tuple_count = tuple_count / 2;

                    let mut it = BTreeLeafPageReverseIterator::new(&page);
                    let mut delete_indexes: Vec<usize> = Vec::new();
                    for (i, tuple) in
                        it.by_ref().take(move_tuple_count).enumerate()
                    {
                        delete_indexes.push(tuple_count - i - 1);
                        new_sibling.insert_tuple(&tuple);
                    }
                    let tuple = it.next().unwrap();
                    key = tuple.get_field(self.key_field).value;

                    for i in &delete_indexes {
                        page.delete_tuple(i);
                    }

                    // do a check
                    if page.empty_slots_count() != delete_indexes.len() {
                        panic!("{}", page.empty_slots_count());
                    }
                }
                SplitStrategy::AddLeftWithoutMove => todo!(),
                SplitStrategy::AddRightWithoutMove => {
                    let mut it = BTreeLeafPageReverseIterator::new(&page);
                    key = it.next().unwrap().get_field(self.key_field).value;
                }
            }

            // set sibling id
            page.set_right_sibling_pid(&new_sibling.get_page_id().page_index);

            // get parent pid for use later
            parent_pid = page.get_parent_pid();
        }
        // borrow of new_sibling_rc end here
        // borrow of page_rc end here

        // 2. Copy the middle key up into the parent page, and
        // recursively split the parent as needed to accommodate
        // the new entry.
        //
        // We put this method outside all the borrow blocks since
        // once the parent page is split, a lot of children will
        // been borrowed. (may including the current leaf page)
        let parent_rc = self.get_parent_with_empty_slots(parent_pid, field);

        // borrow of parent_rc start here
        // borrow of page_rc start here
        // borrow of new_sibling_rc start here
        {
            let mut parent = (*parent_rc).borrow_mut();
            let mut page = (*page_rc).borrow_mut();
            let mut new_sibling = (*new_sibling_rc).borrow_mut();
            let entry = Entry::new(
                key,
                &page.get_page_id(),
                &new_sibling.get_page_id(),
            );
            parent.insert_entry(&entry);

            // set parent id
            page.set_parent_id(&parent.get_page_id());
            new_sibling.set_parent_id(&parent.get_page_id());
        }
        // borrow of parent_rc end here
        // borrow of page_rc end here
        // borrow of new_sibling_rc end here

        if field.value > key {
            new_sibling_rc
        } else {
            page_rc
        }
    }

    fn get_empty_page_index(&self) -> usize {
        let index = self.page_index.get() + 1;
        self.page_index.set(index);
        index
    }

    /**
    Method to encapsulate the process of getting a parent page
    ready to accept new entries.

    This may mean creating a page to become the new root of
    the tree, splitting the existing parent page if there are
    no empty slots, or simply locking and returning the existing
    parent page.

    # Arguments
    * `field`: the key field of the tuple to be inserted after the split is complete. Necessary to know which of the two pages to return.
    * `parentId`: the id of the parent. May be an internal page or the RootPtr page
    */
    fn get_parent_with_empty_slots(
        &self,
        parent_id: BTreePageID,
        field: IntField,
    ) -> Rc<RefCell<BTreeInternalPage>> {
        // create a parent node if necessary
        // this will be the new root of the tree
        match parent_id.category {
            PageCategory::RootPointer => {
                let new_parent_rc = self.get_empty_interanl_page();

                // borrow of new_parent_rc start here
                {
                    let new_parent = (*new_parent_rc).borrow_mut();

                    // update the root pointer
                    let page_id = BTreePageID::new(
                        PageCategory::RootPointer,
                        self.table_id,
                        0,
                    );
                    let root_pointer_page = BufferPool::global()
                        .get_root_pointer_page(&page_id)
                        .unwrap();

                    (*root_pointer_page)
                        .borrow_mut()
                        .set_root_pid(&new_parent.get_page_id());
                }
                // borrow of new_parent_rc end here

                new_parent_rc
            }
            PageCategory::Internal => {
                let parent_rc =
                    BufferPool::global().get_internal_page(&parent_id).unwrap();
                let empty_slots_count: usize;

                // borrow of parent_rc start here
                {
                    empty_slots_count =
                        (*parent_rc).borrow().empty_slots_count();
                }
                // borrow of parent_rc end here

                if empty_slots_count > 0 {
                    return parent_rc;
                } else {
                    // split upper parent
                    return self.split_internal_page(parent_rc, field);
                }
            }
            _ => {
                todo!()
            }
        }
    }

    /**
    Split an internal page to make room for new entries and recursively split its parent page
    as needed to accommodate a new entry. The new entry for the parent should have a key matching
    the middle key in the original internal page being split (this key is "pushed up" to the parent).

    Make a left sibling page and move half of entries to it.

    The child pointers of the new parent entry should point to the two internal pages resulting
    from the split. Update parent pointers as needed.

    Return the internal page into which an entry with key field "field" should be inserted

    # Arguments
    * `field`: the key field of the tuple to be inserted after the split is complete. Necessary to know which of the two pages to return.
    */
    fn split_internal_page(
        &self,
        page_rc: Rc<RefCell<BTreeInternalPage>>,
        field: IntField,
    ) -> Rc<RefCell<BTreeInternalPage>> {
        let sibling_rc = self.get_empty_interanl_page();
        let key: i32;

        // borrow of sibling_rc start here
        // borrow of page_rc start here
        {
            let mut sibling = (*sibling_rc).borrow_mut();
            let mut page = (*page_rc).borrow_mut();

            let mut parent_pid = page.get_parent_pid();

            if parent_pid.category == PageCategory::RootPointer {
                // create new parent page if the parent page is root pointer page.
                let parent_rc = self.get_empty_interanl_page();
                parent_pid = (*parent_rc).borrow().get_page_id();

                // update root pointer page
                // update the root pointer
                let root_pointer_pid = BTreePageID::new(
                    PageCategory::RootPointer,
                    self.table_id,
                    0,
                );
                let root_pointer_page = BufferPool::global()
                    .get_root_pointer_page(&root_pointer_pid)
                    .unwrap();
                (*root_pointer_page).borrow_mut().set_root_pid(&parent_pid);
            }

            let enties_count = page.entries_count();
            let move_entries_count = enties_count / 2;

            let mut delete_indexes: Vec<usize> = Vec::new();
            let mut it = BTreeInternalPageIterator::new(&page);
            let mut entry: Option<Entry> = None;
            for (i, e) in it.by_ref().take(move_entries_count).enumerate() {
                delete_indexes.push(i + 1); // entries index start from 1
                sibling.insert_entry(&e);
                entry = Some(e);

                // set parent id for left child
                let left_pid = e.get_left_child();
                Self::set_parent(&left_pid, &sibling.get_page_id());
            }

            for i in delete_indexes {
                page.delete_entry(i);
            }

            key = entry.unwrap().key;

            let the_last_right_pid = entry.unwrap().get_right_child();
            Self::set_parent(&the_last_right_pid, &sibling.get_page_id());

            let new_entry =
                Entry::new(key, &sibling.get_page_id(), &page.get_page_id());

            let parent_rc = self.get_parent_with_empty_slots(parent_pid, field);

            // borrow of parent_rc start here
            {
                let mut parent = (*parent_rc).borrow_mut();
                parent.insert_entry(&new_entry);
            }
            // borrow of parent_rc end here
        }
        // borrow of sibling_rc end here
        // borrow of page_rc end here

        if field.value > key {
            page_rc
        } else {
            sibling_rc
        }
    }

    fn set_parent(child_pid: &BTreePageID, parent_pid: &BTreePageID) {
        match child_pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let left_rc =
                    BufferPool::global().get_internal_page(&child_pid).unwrap();

                // borrow of left_rc start here
                {
                    let mut left = (*left_rc).borrow_mut();
                    left.set_parent_id(&parent_pid);
                }
                // borrow of left_rc end here
            }
            PageCategory::Leaf => {
                let child_rc =
                    BufferPool::global().get_leaf_page(&child_pid).unwrap();

                // borrow of left_rc start here
                {
                    let mut child = (*child_rc).borrow_mut();
                    child.set_parent_id(&parent_pid);
                }
                // borrow of left_rc end here
            }
            PageCategory::Header => todo!(),
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

    # Arguments
    * tid  - the transaction id
    * pid  - the current page being searched
    * perm - the permissions with which to lock the leaf page
    * f    - the field to search for

    # Return
    * the left-most leaf page possibly containing the key field f
    */
    pub fn find_leaf_page(
        &self,
        page_id: BTreePageID,
        field: Option<IntField>,
    ) -> Rc<RefCell<BTreeLeafPage>> {
        match page_id.category {
            PageCategory::Leaf => {
                // get page and return directly
                return BufferPool::global().get_leaf_page(&page_id).unwrap();
            }
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(&page_id).unwrap();
                let mut child_pid: Option<BTreePageID> = None;

                // borrow of page_rc start here
                {
                    let page = (*page_rc).borrow();
                    let it = BTreeInternalPageIterator::new(&page);
                    let mut entry: Option<Entry> = None;
                    let mut found = false;
                    for e in it {
                        match field {
                            Some(f) => {
                                if e.key >= f.value {
                                    child_pid = Some(e.get_left_child());
                                    found = true;
                                    break;
                                }
                            }
                            None => {
                                child_pid = Some(e.get_left_child());
                                found = true;
                                break;
                            }
                        }
                        entry = Some(e);
                    }

                    if !found {
                        // return right of last entry
                        match entry {
                            Some(e) => {
                                child_pid = Some(e.get_right_child());
                            }
                            None => todo!(),
                        }
                    }
                }
                // borrow of page_rc end here

                match child_pid {
                    Some(child_pid) => {
                        return self.find_leaf_page(child_pid, field);
                    }
                    None => todo!(),
                }
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
            let empty_root_pointer_data =
                BTreeRootPointerPage::empty_page_data();
            let empty_leaf_data = BTreePage::empty_page_data();
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

    fn get_empty_leaf_page(&self) -> Rc<RefCell<BTreeLeafPage>> {
        // create the new page
        let page_index = self.get_empty_page_index();
        let page_id =
            BTreePageID::new(PageCategory::Leaf, self.table_id, page_index);
        let page = BTreeLeafPage::new(
            &page_id,
            BTreePage::empty_page_data().to_vec(),
            &self.tuple_scheme,
            self.key_field,
        );

        self.write_page_to_disk(&page_id);

        let page_rc = Rc::new(RefCell::new(page));

        BufferPool::global()
            .leaf_buffer
            .insert(page_id, page_rc.clone());

        page_rc
    }

    fn get_empty_interanl_page(&self) -> Rc<RefCell<BTreeInternalPage>> {
        // create the new page
        let page_index = self.get_empty_page_index();
        let page_id =
            BTreePageID::new(PageCategory::Internal, self.table_id, page_index);
        let page = BTreeInternalPage::new(
            &page_id,
            BTreePage::empty_page_data().to_vec(),
            &self.tuple_scheme,
            self.key_field,
        );

        self.write_page_to_disk(&page_id);

        let page_rc = Rc::new(RefCell::new(page));

        BufferPool::global()
            .internal_buffer
            .insert(page_id, page_rc.clone());

        page_rc
    }

    fn write_page_to_disk(&self, page_id: &BTreePageID) {
        if WRITE_DISK {
            info!("crate new page and write it to disk, pid: {}", page_id);
            let start_pos = BTreeRootPointerPage::page_size()
                + (page_id.page_index - 1) * PAGE_SIZE;
            self.get_file()
                .seek(SeekFrom::Start(start_pos as u64))
                .expect("io error");
            self.get_file()
                .write(&BTreePage::empty_page_data())
                .expect("io error");
            self.get_file().flush().expect("io error");
        } else {
            info!("crate new page, pid: {}", page_id);
        }
    }

    fn get_first_page(&self) -> Rc<RefCell<BTreeLeafPage>> {
        let page_id = self.get_root_pid();
        return self.find_leaf_page(page_id, None);
    }

    /**
    Get the root page pid.
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
            .expect("io error");
        let page = (*page_ref).borrow();
        let mut root_pid = page.get_root_pid();
        root_pid.table_id = self.get_id();
        root_pid
    }

    /**
    The count of pages in this BTreeFile

    (BTreeRootPointerPage is not included)
    */
    pub fn pages_count(&self) -> usize {
        if WRITE_DISK {
            let file_len = self.get_file().metadata().unwrap().len() as usize;
            (file_len - BTreeRootPointerPage::page_size()) / PAGE_SIZE
        } else {
            BufferPool::global().leaf_buffer.len()
                + BufferPool::global().internal_buffer.len()
        }
    }

    /**
    used for debug
    */
    pub fn draw_tree(&self) {
        println!("\n----- PRINT TREE STRUCTURE START -----\n");

        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        println!("root pointer: {}", root_pointer_pid);

        let root_pid = self.get_root_pid();
        println!("├── root: {}", root_pid);
        self.draw_subtree(&root_pid, 1);

        println!("\n----- PRINT TREE STRUCTURE END   -----\n");
    }

    pub fn draw_subtree(&self, pid: &BTreePageID, level: usize) {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => self.draw_internal_node(pid, level),
            PageCategory::Leaf => self.draw_leaf_node(pid, level),
            PageCategory::Header => todo!(),
        }
    }

    pub fn draw_leaf_node(&self, pid: &BTreePageID, level: usize) {
        let prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_leaf_page(&pid).unwrap();

        let mut it = BTreeLeafPageIterator::new(Rc::clone(&page_rc));
        let first_tuple = it.next().unwrap();

        let page = (*page_rc).borrow();
        let mut rit = BTreeLeafPageReverseIterator::new(&page);
        let last_tuple = rit.next().unwrap();

        println!("{}├── leaf: {}", prefix, page.get_page_id());
        println!("{}├── first: {}", prefix, first_tuple);
        println!("{}└── last:  {}", prefix, last_tuple);
    }

    pub fn draw_internal_node(&self, pid: &BTreePageID, level: usize) {
        let prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_internal_page(&pid).unwrap();

        // borrow of page_rc start here
        {
            let page = (*page_rc).borrow();
            println!("{}├── internal: {}", prefix, page.get_page_id());
            let it = BTreeInternalPageIterator::new(&page);
            for (i, entry) in it.enumerate() {
                self.draw_entry(i, &entry, level)
            }
        }
        // borrow of page_rc end here
    }

    pub fn draw_entry(&self, id: usize, entry: &Entry, level: usize) {
        let prefix = "│   ".repeat(level);
        match entry.get_right_child().category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                self.draw_internal_node(&entry.get_left_child(), level + 1);
                println!("{}├── key: {}", prefix, entry.key);
                self.draw_internal_node(&entry.get_right_child(), level + 1);
            }
            PageCategory::Leaf => {
                if id == 0 {
                    let left = self.get_leaf_recap(&entry.get_left_child());
                    println!(
                        "{}├── ---: {}, left: {}",
                        prefix, entry.key, left
                    );
                }
                let right = self.get_leaf_recap(&entry.get_right_child());
                println!("{}├── key: {}, right: {}", prefix, entry.key, right);
            }
            PageCategory::Header => todo!(),
        }
    }

    fn get_leaf_recap(&self, page_id: &BTreePageID) -> String {
        let page_rc = BufferPool::global().get_leaf_page(&page_id).unwrap();

        let mut it = BTreeLeafPageIterator::new(Rc::clone(&page_rc));
        let first_tuple = it.next().unwrap();

        let page = (*page_rc).borrow();
        let mut rit = BTreeLeafPageReverseIterator::new(&page);
        let last_tuple = rit.next().unwrap();

        return format!(
            "[leaf {}, {} - {}]",
            page_id.page_index, first_tuple, last_tuple
        );
    }
}

pub struct BTreeTableIterator {
    page_rc: Rc<RefCell<BTreeLeafPage>>,
    page_it: BTreeLeafPageIterator,
}

impl BTreeTableIterator {
    pub fn new(table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page();

        Self {
            page_rc: Rc::clone(&page_rc),
            page_it: BTreeLeafPageIterator::new(Rc::clone(&page_rc)),
        }
    }
}

impl Iterator for BTreeTableIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.page_it.next();
        if !v.is_none() {
            return v;
        }

        let right = (*self.page_rc).borrow().get_right_sibling_pid();
        match right {
            Some(right) => {
                let sibling_rc =
                    BufferPool::global().get_leaf_page(&right).unwrap();
                let page_it =
                    BTreeLeafPageIterator::new(Rc::clone(&sibling_rc));

                self.page_rc = Rc::clone(&sibling_rc);
                self.page_it = page_it;
                return self.page_it.next();
            }
            None => {
                return None;
            }
        }
    }
}

pub enum Op {
    Equals,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
    Like,
    NotEquals,
}

pub struct Predicate {
    pub op: Op,
    pub field: IntField,
}

impl Predicate {
    pub fn new(op: Op, field: IntField) -> Self {
        Self { op, field }
    }
}

pub struct BTreeTableSearchIterator {
    current_page_rc: Rc<RefCell<BTreeLeafPage>>,
    page_it: BTreeLeafPageIterator,
    predicate: Predicate,
    key_field: usize,
}

impl BTreeTableSearchIterator {
    pub fn new(table: &BTreeTable, index_predicate: Predicate) -> Self {
        let rc: Rc<RefCell<BTreeLeafPage>>;

        let root_pid = table.get_root_pid();

        match index_predicate.op {
            Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
                rc = table.find_leaf_page(root_pid, Some(index_predicate.field))
            }
            Op::LessThan | Op::LessThanOrEq => {
                rc = table.find_leaf_page(root_pid, None)
            }
            Op::Like => todo!(),
            Op::NotEquals => todo!(),
        }

        Self {
            current_page_rc: Rc::clone(&rc),
            page_it: BTreeLeafPageIterator::new(Rc::clone(&rc)),
            predicate: index_predicate,
            key_field: table.key_field,
        }
    }
}

impl Iterator for BTreeTableSearchIterator {
    type Item = Tuple;

    // TODO: Short circuit on some conditions.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let tuple = self.page_it.next();
            match tuple {
                Some(t) => match self.predicate.op {
                    Op::Equals => {
                        let field = t.get_field(self.key_field);
                        if field == self.predicate.field {
                            return Some(t);
                        } else if field > self.predicate.field {
                            return None;
                        }
                    }
                    Op::GreaterThan => {
                        let field = t.get_field(self.key_field);
                        if field > self.predicate.field {
                            return Some(t);
                        }
                    }
                    Op::GreaterThanOrEq => {
                        let field = t.get_field(self.key_field);
                        if field >= self.predicate.field {
                            return Some(t);
                        }
                    }
                    Op::LessThan => {
                        let field = t.get_field(self.key_field);
                        if field < self.predicate.field {
                            return Some(t);
                        } else if field >= self.predicate.field {
                            return None;
                        }
                    }
                    Op::LessThanOrEq => {
                        let field = t.get_field(self.key_field);
                        if field <= self.predicate.field {
                            return Some(t);
                        } else if field > self.predicate.field {
                            return None;
                        }
                    }
                    Op::Like => todo!(),
                    Op::NotEquals => todo!(),
                },
                None => {
                    // init iterator on next page and continue search
                    let right = (*self.current_page_rc)
                        .borrow()
                        .get_right_sibling_pid();
                    match right {
                        Some(pid) => {
                            let rc = BufferPool::global()
                                .get_leaf_page(&pid)
                                .unwrap();
                            self.current_page_rc = Rc::clone(&rc);
                            self.page_it =
                                BTreeLeafPageIterator::new(Rc::clone(&rc));
                            continue;
                        }
                        None => {
                            return None;
                        }
                    }
                }
            }
        }
    }
}
