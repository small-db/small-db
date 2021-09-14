use super::{
    buffer_pool::BufferPool,
    page::{
        empty_page_data, BTreeInternalPage, BTreeLeafPage,
        BTreeLeafPageIterator, BTreeLeafPageIteratorRc,
        BTreeLeafPageReverseIterator, BTreePageID, BTreeRootPointerPage, Entry,
    },
    tuple::WrappedTuple,
};
use crate::{
    btree::page::{
        BTreeBasePage, BTreeInternalPageIterator,
        BTreeInternalPageReverseIterator, PageCategory,
    },
    field::IntField,
};

use core::fmt;
use log::info;
use std::{cell::Cell, str, time::SystemTime};

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

use super::tuple::{Tuple, TupleScheme};

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
}

#[derive(Copy, Clone)]
pub enum WriteScene {
    Random,
    Sequential,
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
        row_scheme: &TupleScheme,
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
        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);
        // unix_time.

        let table_id = hasher.finish() as i32;

        Self::file_init(f.borrow_mut());

        Self {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme.clone(),
            file: f,
            table_id,

            /*
            start from 1 (the root page)

            TODO: init it according to actual condition
            */
            page_index: Cell::new(1),
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    /// May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(&self, tuple: &Tuple) {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let field = tuple.get_field(self.key_field);
        let mut leaf_rc = self.find_leaf_page(root_pid, Some(field));

        if leaf_rc.borrow().empty_slots_count() == 0 {
            leaf_rc =
                self.split_leaf_page(leaf_rc, tuple.get_field(self.key_field));
        }
        leaf_rc.borrow_mut().insert_tuple(&tuple);
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
        let key: IntField;

        // borrow of new_sibling_rc start here
        // borrow of page_rc start here
        {
            let mut new_sibling = new_sibling_rc.borrow_mut();
            let mut page = page_rc.borrow_mut();
            // 1. adding a new page on the right of the existing
            // page and moving half of the tuples to the new page
            let tuple_count = page.tuples_count();
            let move_tuple_count = tuple_count / 2;

            let mut it = BTreeLeafPageReverseIterator::new(&page);
            let mut delete_indexes: Vec<usize> = Vec::new();
            for tuple in it.by_ref().take(move_tuple_count) {
                delete_indexes.push(tuple.get_slot_number());
                new_sibling.insert_tuple(&tuple);
            }

            for i in delete_indexes {
                page.delete_tuple(i);
            }

            let mut it = BTreeLeafPageReverseIterator::new(&page);
            key = it.next().unwrap().get_field(self.key_field);

            // set sibling id
            new_sibling.set_right_sibling_pid(page.get_right_sibling_pid());
            new_sibling.set_left_sibling_pid(Some(page.get_pid()));
            page.set_right_sibling_pid(Some(new_sibling.get_pid()));

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
            let mut parent = parent_rc.borrow_mut();
            let mut page = page_rc.borrow_mut();
            let mut new_sibling = new_sibling_rc.borrow_mut();
            let mut entry =
                Entry::new(key, &page.get_pid(), &new_sibling.get_pid());
            parent.insert_entry(&mut entry);

            // set parent id
            page.set_parent_pid(&parent.get_page_id());
            new_sibling.set_parent_pid(&parent.get_page_id());
        }
        // borrow of parent_rc end here
        // borrow of page_rc end here
        // borrow of new_sibling_rc end here

        if field > key {
            new_sibling_rc
        } else {
            page_rc
        }
    }

    pub fn get_empty_page_index(&self) -> usize {
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
                    let new_parent = new_parent_rc.borrow_mut();

                    // update the root pointer
                    let page_id = BTreePageID::new(
                        PageCategory::RootPointer,
                        self.table_id,
                        0,
                    );
                    let root_pointer_page = BufferPool::global()
                        .get_root_pointer_page(&page_id)
                        .unwrap();

                    root_pointer_page
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
                    empty_slots_count = parent_rc.borrow().empty_slots_count();
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

    Make a right sibling page and move half of entries to it.

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
        info!("split start");
        self.draw_tree(-1);

        let sibling_rc = self.get_empty_interanl_page();
        let key: IntField;
        let mut parent_pid: BTreePageID;
        let mut new_entry: Entry;

        // borrow of sibling_rc start here
        // borrow of page_rc start here
        {
            let mut sibling = sibling_rc.borrow_mut();
            let mut page = page_rc.borrow_mut();

            parent_pid = page.get_parent_pid();

            if parent_pid.category == PageCategory::RootPointer {
                // create new parent page if the parent page is root pointer
                // page.
                let parent_rc = self.get_empty_interanl_page();
                parent_pid = parent_rc.borrow().get_page_id();

                // update the root pointer
                let root_pointer_pid = BTreePageID::new(
                    PageCategory::RootPointer,
                    self.table_id,
                    0,
                );
                let root_pointer_page = BufferPool::global()
                    .get_root_pointer_page(&root_pointer_pid)
                    .unwrap();
                root_pointer_page.borrow_mut().set_root_pid(&parent_pid);
            }

            let enties_count = page.entries_count();
            let move_entries_count = enties_count / 2;

            let mut delete_indexes: Vec<usize> = Vec::new();
            let mut it = BTreeInternalPageReverseIterator::new(&page);
            for e in it.by_ref().take(move_entries_count) {
                delete_indexes.push(e.get_record_id());
                sibling.insert_entry(&e);

                // set parent id for right child
                let right_pid = e.get_right_child();
                Self::set_parent(&right_pid, &sibling.get_page_id());
            }

            let middle_entry = it.next().unwrap();

            // also delete the middle entry
            delete_indexes.push(middle_entry.get_record_id());
            info!("delete_indexes: {:?}", delete_indexes);
            for i in delete_indexes {
                page.delete_entry(i);
            }

            // set parent id for right child to the middle entry
            Self::set_parent(
                &middle_entry.get_right_child(),
                &sibling.get_page_id(),
            );

            key = middle_entry.get_key();
            new_entry =
                Entry::new(key, &page.get_page_id(), &sibling.get_page_id());
        }
        // borrow of sibling_rc end here
        // borrow of page_rc end here

        let parent_rc = self.get_parent_with_empty_slots(parent_pid, field);

        // borrow of parent_rc start here
        // borrow of page_rc start here
        // borrow of sibling_rc start here
        {
            let mut parent = parent_rc.borrow_mut();
            info!("entry: {}", new_entry);
            parent.insert_entry(&mut new_entry);

            let mut page = page_rc.borrow_mut();
            let mut sibling = sibling_rc.borrow_mut();
            page.set_parent_pid(&parent.get_page_id());
            sibling.set_parent_pid(&parent.get_page_id());
        }
        // borrow of parent_rc end here
        // borrow of page_rc end here
        // borrow of sibling_rc end here

        info!("split end");
        self.draw_tree(-1);

        if field > key {
            sibling_rc
        } else {
            page_rc
        }
    }

    /**
    Delete a tuple from this BTreeFile.

    May cause pages to merge or redistribute entries/tuples if the pages
    become less than half full.
    */
    pub fn delete_tuple(&self, tuple: Rc<WrappedTuple>) {
        let pid = (*tuple).get_pid();
        let leaf_rc = BufferPool::global().get_leaf_page(&pid).unwrap();

        // borrow of leaf_rc start here
        {
            let mut leaf = leaf_rc.borrow_mut();
            leaf.delete_tuple(tuple.get_slot_number());

            // if the page is below minimum occupancy, get some tuples from its siblings
            // or merge with one of the siblings
            let max_empty_slots = leaf.slot_count - leaf.slot_count / 2; // ceiling
            if leaf.empty_slots_count() > max_empty_slots {
                self.handle_min_occupancy_page(&pid);
            }
        }
        // borrow of leaf_rc end here
    }

    /**
    Handle the case when a B+ tree page becomes less than half full due to deletions.
    If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
    Otherwise merge with one of the siblings. Update pointers as needed.
    */
    fn handle_min_occupancy_page(&self, pid: &BTreePageID) {
        match pid.category {
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(&pid).unwrap();
                self.handle_min_occupancy_internal_page(page_rc);
            }
            PageCategory::Leaf => {
                unimplemented!()
            }
            _ => {
                panic!("handle_min_occupancy_page: invalid page category");
            }
        }
    }

    /**
    Handle the case when a leaf page becomes less than half full due to deletions.
    If one of its siblings has extra tuples, redistribute those tuples.
    Otherwise merge with one of the siblings. Update pointers as needed.

    # Arguments

    * `page_rc` - the leaf page to handle
    * `left_entry` - the entry in the parent pointing to the given page and its left-sibling
    * `right_entry` - the entry in the parent pointing to the given page and its right-sibling

    */
    fn handle_min_occupancy_leaf_page(
        &self,
        page_rc: Rc<RefCell<BTreeLeafPage>>,
        left_entry: Option<Entry>,
        right_entry: Option<Entry>,
    ) {
        let parent_rc = BufferPool::global()
            .get_internal_page(&page_rc.borrow().get_parent_pid())
            .unwrap();

        // borrow of page_rc start here
        {
            let page = page_rc.borrow();
            if let Some(left_pid) = page.get_left_sibling_pid() {
                let left_rc =
                    BufferPool::global().get_leaf_page(&left_pid).unwrap();
                // borrow of left_rc start here
                {
                    let mut left = left_rc.borrow_mut();
                    if left.empty_slots_count() > left.max_stable_empty_slots()
                    {
                        unimplemented!()
                    } else {
                    }
                }
                // borrow of left_rc end here
            }
        }
        // borrow of page_rc end here
    }

    /**
    Merge two leaf pages by moving all tuples from the right page to the left page.
    Delete the corresponding key and right child pointer from the parent, and recursively
    handle the case when the parent gets below minimum occupancy.
    Update sibling pointers as needed, and make the right page available for reuse.

    # Arguments

    - `left_page`    - the left leaf page
    - `right_page`   - the right leaf page
    - `parent`      - the parent of the two pages
    - `parent_entry` - the entry in the parent corresponding to the left_page and right_page

    */
    fn merge_leaf_pages(
        &self,
        left_page_rc: Rc<RefCell<BTreeLeafPage>>,
        right_page_rc: Rc<RefCell<BTreeLeafPage>>,
        parent_rc: Rc<RefCell<BTreeInternalPage>>,
        parent_entry: &Entry,
    ) {
        unimplemented!()
    }

    /**
    Steal tuples from a sibling and copy them to the given page so that both pages are at least
    half full.  Update the parent's entry so that the key matches the key field of the first
    tuple in the right-hand page.
    */
    fn steal_from_leaf_page(&self) {
        unimplemented!()
    }

    /**
    Handle the case when an internal page becomes less than half full due to deletions.
    If one of its siblings has extra entries, redistribute those entries.
    Otherwise merge with one of the siblings. Update pointers as needed.
    */
    fn handle_min_occupancy_internal_page(
        &self,
        page: Rc<RefCell<BTreeInternalPage>>,
    ) {
        unimplemented!();
    }

    pub fn set_root_pid(&self, root_pid: &BTreePageID) {
        let root_pointer_pid =
            BTreePageID::new(PageCategory::RootPointer, self.table_id, 0);
        let root_pointer_rc = BufferPool::global()
            .get_root_pointer_page(&root_pointer_pid)
            .unwrap();
        root_pointer_rc.borrow_mut().set_root_pid(root_pid);
    }

    fn set_parent(child_pid: &BTreePageID, parent_pid: &BTreePageID) {
        match child_pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let left_rc =
                    BufferPool::global().get_internal_page(&child_pid).unwrap();

                // borrow of left_rc start here
                {
                    let mut left = left_rc.borrow_mut();
                    left.set_parent_pid(&parent_pid);
                }
                // borrow of left_rc end here
            }
            PageCategory::Leaf => {
                let child_rc =
                    BufferPool::global().get_leaf_page(&child_pid).unwrap();

                // borrow of left_rc start here
                {
                    let mut child = child_rc.borrow_mut();
                    child.set_parent_pid(&parent_pid);
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
                    let page = page_rc.borrow();
                    let it = BTreeInternalPageIterator::new(&page);
                    let mut entry: Option<Entry> = None;
                    let mut found = false;
                    for e in it {
                        match field {
                            Some(f) => {
                                if e.get_key() >= f {
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
    fn file_init(mut file: RefMut<File>) {
        // if db file is empty, create root pointer page at first
        if file.metadata().unwrap().len() == 0 {
            // write root pointer page
            {
                // set the root pid to 1
                let mut data = empty_page_data();
                let root_pid_bytes = 1_i32.to_le_bytes();
                for i in 0..4 {
                    data[i] = root_pid_bytes[i];
                }
                file.write(&data).unwrap();
            }

            // write the first leaf page
            {
                let data = BTreeBasePage::empty_page_data();
                file.write(&data).unwrap();
            }
        }
    }

    fn get_empty_leaf_page(&self) -> Rc<RefCell<BTreeLeafPage>> {
        // create the new page
        let page_index = self.get_empty_page_index();
        let page_id =
            BTreePageID::new(PageCategory::Leaf, self.table_id, page_index);
        let page = BTreeLeafPage::new(
            &page_id,
            BTreeBasePage::empty_page_data().to_vec(),
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
            BTreeBasePage::empty_page_data().to_vec(),
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

    pub fn write_page_to_disk(&self, page_id: &BTreePageID) {
        let start_pos: usize = page_id.page_index * BufferPool::get_page_size();
        self.get_file()
            .seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");
        self.get_file()
            .write(&BTreeBasePage::empty_page_data())
            .expect("io error");
        self.get_file().flush().expect("io error");
    }

    pub fn get_first_page(&self) -> Rc<RefCell<BTreeLeafPage>> {
        let page_id = self.get_root_pid();
        return self.find_leaf_page(page_id, None);
    }

    /**
    Get the root page pid.
    */
    pub fn get_root_pid(&self) -> BTreePageID {
        let root_ptr_rc = self.get_root_ptr_page();
        let mut root_pid = root_ptr_rc.borrow().get_root_pid();
        root_pid.table_id = self.get_id();
        root_pid
    }

    pub fn get_root_ptr_page(&self) -> Rc<RefCell<BTreeRootPointerPage>> {
        let root_ptr_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        BufferPool::global()
            .get_root_pointer_page(&root_ptr_pid)
            .unwrap()
    }

    /**
    The count of pages in this BTreeFile

    (BTreeRootPointerPage is not included)
    */
    pub fn pages_count(&self) -> usize {
        let file_size = self.file.borrow().metadata().unwrap().len() as usize;
        file_size / BufferPool::get_page_size() - 1
    }

    // get the first tuple under the internal/leaf page
    pub fn get_first_tuple(&self, _pid: &BTreePageID) -> Option<Tuple> {
        todo!()
    }

    pub fn set_page_index(&self, i: usize) {
        self.page_index.set(i);
    }

    // get the last tuple under the internal/leaf page
    pub fn get_last_tuple(&self, pid: &BTreePageID) -> Option<WrappedTuple> {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(pid).unwrap();

                // borrow of page_rc start here
                let child_pid: BTreePageID;
                {
                    let page = page_rc.borrow();
                    let mut it = BTreeInternalPageReverseIterator::new(&page);
                    child_pid = it.next().unwrap().get_right_child();
                }
                // borrow of page_rc end here
                self.get_last_tuple(&child_pid)
            }
            PageCategory::Leaf => {
                let page_rc = BufferPool::global().get_leaf_page(pid).unwrap();

                let page = page_rc.borrow();
                let mut it = BTreeLeafPageReverseIterator::new(&page);
                it.next()
            }
            PageCategory::Header => todo!(),
        }
    }

    /**
    used for debug

    # Arguments
    * `max_level` - the max level of the print, -1 for print all
    */
    pub fn draw_tree(&self, max_level: i32) {
        println!("\n----- PRINT TREE STRUCTURE START -----\n");

        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        println!("root pointer: {}", root_pointer_pid);

        let root_pid = self.get_root_pid();
        self.draw_subtree(&root_pid, 0, max_level);

        println!("\n----- PRINT TREE STRUCTURE END   -----\n");
    }

    fn draw_subtree(&self, pid: &BTreePageID, level: usize, max_level: i32) {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                self.draw_internal_node(pid, level, max_level)
            }
            PageCategory::Leaf => self.draw_leaf_node(pid, level),
            PageCategory::Header => todo!(),
        }
    }

    fn draw_leaf_node(&self, pid: &BTreePageID, level: usize) {
        let prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_leaf_page(&pid).unwrap();

        let mut it = BTreeLeafPageIteratorRc::new(Rc::clone(&page_rc));
        let first_tuple = it.next().unwrap();

        let page = page_rc.borrow();
        let mut rit = BTreeLeafPageReverseIterator::new(&page);
        let last_tuple = rit.next().unwrap();

        println!(
            "{}├── leaf: {} ({} tuples)",
            prefix,
            page.get_pid(),
            page.tuples_count()
        );
        println!("{}├── first: {}", prefix, first_tuple);
        println!("{}└── last:  {}", prefix, last_tuple);
    }

    fn draw_internal_node(
        &self,
        pid: &BTreePageID,
        level: usize,
        max_level: i32,
    ) {
        let prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_internal_page(&pid).unwrap();

        // borrow of page_rc start here
        {
            let page = page_rc.borrow();
            println!(
                "{}├── internal: {} ({} entries)",
                prefix,
                pid,
                page.entries_count()
            );
            if max_level != -1 && level as i32 == max_level {
                return;
            }
            let it = BTreeInternalPageIterator::new(&page);
            for (i, entry) in it.enumerate() {
                self.draw_entry(i, &entry, level + 1, max_level);
            }
        }
        // borrow of page_rc end here
    }

    fn draw_entry(
        &self,
        id: usize,
        entry: &Entry,
        level: usize,
        max_level: i32,
    ) {
        let prefix = "│   ".repeat(level);
        if id == 0 {
            self.draw_subtree(&entry.get_left_child(), level + 1, max_level);
        }
        println!("{}├── key: {}", prefix, entry.get_key());
        self.draw_subtree(&entry.get_right_child(), level + 1, max_level);
    }

    /**
    checks the integrity of the tree:
    - parent pointers.
    - sibling pointers.
    - range invariants.
    - record to page pointers.
    - occupancy invariants. (if enabled)

    panic on any error found.
    */
    pub fn check_integrity(&self, check_occupancy: bool) {
        let root_ptr_page = self.get_root_ptr_page();
        let root_pid = root_ptr_page.borrow().get_root_pid();
        let root_summary = self.check_sub_tree(
            &root_pid,
            &root_ptr_page.borrow().get_pid(),
            None,
            None,
            check_occupancy,
            0,
        );
        assert!(root_summary.left_ptr.is_none());
        assert!(root_summary.right_ptr.is_none());
    }

    /**
    panic on any error found.
    */
    fn check_sub_tree(
        &self,
        pid: &BTreePageID,
        parent_pid: &BTreePageID,
        mut lower_bound: Option<IntField>,
        upper_bound: Option<IntField>,
        check_occupancy: bool,
        depth: usize,
    ) -> SubtreeSummary {
        match pid.category {
            PageCategory::Leaf => {
                let page_rc = BufferPool::global().get_leaf_page(&pid).unwrap();
                let page = page_rc.borrow();
                page.check_integrity(
                    parent_pid,
                    lower_bound,
                    upper_bound,
                    check_occupancy,
                    depth,
                );

                return SubtreeSummary {
                    left_ptr: page.get_left_sibling_pid(),
                    right_ptr: page.get_right_sibling_pid(),

                    left_most_pid: Some(page.get_pid()),
                    right_most_pid: Some(page.get_pid()),

                    depth,
                };
            }

            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(&pid).unwrap();
                let page = page_rc.borrow();
                page.check_integrity(
                    parent_pid,
                    lower_bound,
                    upper_bound,
                    check_occupancy,
                    depth,
                );

                let mut it = BTreeInternalPageIterator::new(&page);
                let current = it.next().unwrap();
                let mut accumulation = self.check_sub_tree(
                    &current.get_left_child(),
                    pid,
                    lower_bound,
                    Some(current.get_key()),
                    check_occupancy,
                    depth + 1,
                );

                let mut last_entry = current;
                for entry in it {
                    let current_summary = self.check_sub_tree(
                        &entry.get_left_child(),
                        pid,
                        lower_bound,
                        Some(entry.get_key()),
                        check_occupancy,
                        depth + 1,
                    );
                    accumulation =
                        accumulation.check_and_merge(&current_summary);

                    lower_bound = Some(entry.get_key());

                    last_entry = entry;
                }

                let last_right_summary = self.check_sub_tree(
                    &last_entry.get_right_child(),
                    pid,
                    lower_bound,
                    upper_bound,
                    check_occupancy,
                    depth + 1,
                );
                accumulation =
                    accumulation.check_and_merge(&last_right_summary);

                return accumulation;
            }

            // no other page types allowed inside the tree.
            _ => panic!("invalid page category"),
        }
    }
}

struct SubtreeSummary {
    /// The distance towards the root.
    depth: usize,

    left_ptr: Option<BTreePageID>,
    left_most_pid: Option<BTreePageID>,
    right_ptr: Option<BTreePageID>,
    right_most_pid: Option<BTreePageID>,
}

impl SubtreeSummary {
    fn check_and_merge(&mut self, right: &SubtreeSummary) -> SubtreeSummary {
        assert_eq!(self.depth, right.depth);
        assert_eq!(self.right_ptr, right.left_most_pid);
        assert_eq!(self.right_most_pid, right.left_ptr);

        let acc = SubtreeSummary {
            depth: self.depth,
            left_ptr: self.left_ptr,
            left_most_pid: self.left_most_pid,
            right_ptr: right.right_ptr,
            right_most_pid: right.right_most_pid,
        };
        return acc;
    }
}

pub struct BTreeTableIterator {
    page_rc: Rc<RefCell<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,
}

impl BTreeTableIterator {
    pub fn new(table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page();

        Self {
            page_rc: Rc::clone(&page_rc),
            page_it: BTreeLeafPageIteratorRc::new(Rc::clone(&page_rc)),
        }
    }
}

impl Iterator for BTreeTableIterator {
    type Item = Rc<WrappedTuple>;

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
                    BTreeLeafPageIteratorRc::new(Rc::clone(&sibling_rc));

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
    page_it: BTreeLeafPageIteratorRc,
    predicate: Predicate,
    key_field: usize,
}

impl<'t> BTreeTableSearchIterator {
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
            page_it: BTreeLeafPageIteratorRc::new(Rc::clone(&rc)),
            predicate: index_predicate,
            key_field: table.key_field,
        }
    }
}

impl Iterator for BTreeTableSearchIterator {
    type Item = Rc<WrappedTuple>;

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
                                BTreeLeafPageIteratorRc::new(Rc::clone(&rc));
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
