use std::{env, ops::DerefMut};

use log::{debug};

use super::{
    buffer_pool::BufferPool,
    page::{
        empty_page_data, BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage,
        BTreeLeafPageIterator, BTreeLeafPageIteratorRc, BTreePageID,
        BTreeRootPointerPage, BTreeVirtualPage, Entry,
    },
    tuple::WrappedTuple,
};
use crate::{
    btree::page::{
        BTreeBasePage, BTreeInternalPageIterator, BTreePage, PageCategory,
    },
    error::MyError,
    field::IntField,
};

use core::fmt;
use std::{cell::Cell, cmp, str, time::SystemTime};

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

enum SearchFor {
    IntField(IntField),
    LeftMost,
    RightMost,
}

/// B+ Tree
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

        // let file_size = f.borrow().metadata().unwrap().len() as usize;
        // debug!("btree initialized, file size: {}", file_size);

        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let table_id = hasher.finish() as i32;

        Self::file_init(f.borrow_mut());

        Self {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme.clone(),
            file: f,
            table_id,

            // start from 1 (the root page)
            //
            // TODO: init it according to actual condition
            page_index: Cell::new(1),
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    pub fn get_tuple_scheme(&self) -> TupleScheme {
        self.tuple_scheme.clone()
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
        let mut leaf_rc =
            self.find_leaf_page(root_pid, SearchFor::IntField(field));

        if leaf_rc.borrow().empty_slots_count() == 0 {
            leaf_rc =
                self.split_leaf_page(leaf_rc, tuple.get_field(self.key_field));
        }
        leaf_rc.borrow_mut().insert_tuple(&tuple);
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
    ///
    /// # Arguments
    /// `field`: the key field of the tuple to be inserted after the split is
    /// complete. Necessary to know which of the two pages to return.
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

            let mut it = BTreeLeafPageIterator::new(&page);
            let mut delete_indexes: Vec<usize> = Vec::new();
            for tuple in it.by_ref().rev().take(move_tuple_count) {
                delete_indexes.push(tuple.get_slot_number());
                new_sibling.insert_tuple(&tuple);
            }

            for i in delete_indexes {
                page.delete_tuple(i);
            }

            let mut it = BTreeLeafPageIterator::new(&page);
            key = it.next_back().unwrap().get_field(self.key_field);

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

            // set left pointer for the old right sibling
            if let Some(old_right_pid) = page.get_right_pid() {
                let old_right_rc =
                    BufferPool::global().get_leaf_page(&old_right_pid).unwrap();
                old_right_rc
                    .borrow_mut()
                    .set_left_pid(Some(new_sibling.get_pid()));
            }

            // set sibling id
            new_sibling.set_right_pid(page.get_right_pid());
            new_sibling.set_left_pid(Some(page.get_pid()));
            page.set_right_pid(Some(new_sibling.get_pid()));

            // set parent id
            page.set_parent_pid(&parent.get_pid());
            new_sibling.set_parent_pid(&parent.get_pid());
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
        let root_ptr_rc = self.get_root_ptr_page();
        // borrow of root_ptr_rc start here
        {
            let root_ptr = root_ptr_rc.borrow();
            let header_pid = root_ptr.get_header_pid();
            if let Some(header_pid) = header_pid {
                let header_rc =
                    BufferPool::global().get_header_page(&header_pid).unwrap();
                // borrow of header_rc start here
                {
                    let header = header_rc.borrow();
                    if let Some(i) = header.get_empty_slot() {
                        return i;
                    }
                }
            }
        }
        // borrow of root_ptr_rc end here

        let index = self.page_index.get() + 1;
        self.page_index.set(index);
        index
    }

    /// Method to encapsulate the process of getting a parent page
    /// ready to accept new entries.
    ///
    /// This may mean creating a page to become the new root of
    /// the tree, splitting the existing parent page if there are
    /// no empty slots, or simply locking and returning the existing
    /// parent page.
    ///
    /// # Arguments
    /// `field`: the key field of the tuple to be inserted after the split is
    /// complete. Necessary to know which of the two pages to return.
    /// `parentId`: the id of the parent. May be an internal page or the RootPtr
    /// page
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
                        .set_root_pid(&new_parent.get_pid());
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

    /// Split an internal page to make room for new entries and recursively
    /// split its parent page as needed to accommodate a new entry. The new
    /// entry for the parent should have a key matching the middle key in
    /// the original internal page being split (this key is "pushed up" to the
    /// parent).
    ///
    /// Make a right sibling page and move half of entries to it.
    ///
    /// The child pointers of the new parent entry should point to the two
    /// internal pages resulting from the split. Update parent pointers as
    /// needed.
    ///
    /// Return the internal page into which an entry with key field "field"
    /// should be inserted
    ///
    /// # Arguments
    /// `field`: the key field of the tuple to be inserted after the split is
    /// complete. Necessary to know which of the two pages to return.
    fn split_internal_page(
        &self,
        page_rc: Rc<RefCell<BTreeInternalPage>>,
        field: IntField,
    ) -> Rc<RefCell<BTreeInternalPage>> {
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
                parent_pid = parent_rc.borrow().get_pid();

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
            let mut it = BTreeInternalPageIterator::new(&page);
            for e in it.by_ref().rev().take(move_entries_count) {
                delete_indexes.push(e.get_record_id());
                sibling.insert_entry(&e);

                // set parent id for right child
                let right_pid = e.get_right_child();
                Self::set_parent(&right_pid, &sibling.get_pid());
            }

            let middle_entry = it.next_back().unwrap();

            // also delete the middle entry
            delete_indexes.push(middle_entry.get_record_id());
            for i in delete_indexes {
                page.delete_key_and_right_child(i);
            }

            // set parent id for right child to the middle entry
            Self::set_parent(
                &middle_entry.get_right_child(),
                &sibling.get_pid(),
            );

            key = middle_entry.get_key();
            new_entry = Entry::new(key, &page.get_pid(), &sibling.get_pid());
        }
        // borrow of sibling_rc end here
        // borrow of page_rc end here

        let parent_rc = self.get_parent_with_empty_slots(parent_pid, field);
        parent_pid = parent_rc.borrow().get_pid();
        page_rc.borrow_mut().set_parent_pid(&parent_pid);
        sibling_rc.borrow_mut().set_parent_pid(&parent_pid);

        // borrow of parent_rc start here
        {
            let mut parent = parent_rc.borrow_mut();
            parent.insert_entry(&mut new_entry);
        }
        // borrow of parent_rc end here

        if field > key {
            sibling_rc
        } else {
            page_rc
        }
    }
}

/// delete implementation
impl BTreeTable {
    /// Delete a tuple from this BTreeFile.
    ///
    /// May cause pages to merge or redistribute entries/tuples if the pages
    /// become less than half full.
    pub fn delete_tuple(&self, tuple: &WrappedTuple) -> Result<(), MyError> {
        let pid = tuple.get_pid();
        let leaf_rc = BufferPool::global().get_leaf_page(&pid).unwrap();

        // hold the leaf page
        {
            let mut leaf = leaf_rc.borrow_mut();
            leaf.delete_tuple(tuple.get_slot_number());
        }
        // release the leaf page

        if leaf_rc.borrow().stable() {
            return Ok(());
        } else {
            return self.handle_erratic_leaf_page(leaf_rc);
        }
    }

    /// Handle the case when a leaf page becomes less than half full due to
    /// deletions.
    ///
    /// If one of its siblings has extra tuples, redistribute those tuples.
    /// Otherwise merge with one of the siblings. Update pointers as needed.
    fn handle_erratic_leaf_page(
        &self,
        page_rc: Rc<RefCell<BTreeLeafPage>>,
    ) -> Result<(), MyError> {
        if page_rc.borrow().get_parent_pid().category
            == PageCategory::RootPointer
        {
            return Ok(());
        }

        let left_pid = page_rc.borrow().get_left_pid();
        let right_pid = page_rc.borrow().get_right_pid();

        if let Some(left_pid) = left_pid {
            let left_rc =
                BufferPool::global().get_leaf_page(&left_pid).unwrap();
            self.balancing_two_leaf_pages(left_rc, page_rc)?;
        } else if let Some(right_pid) = right_pid {
            let right_rc =
                BufferPool::global().get_leaf_page(&right_pid).unwrap();
            self.balancing_two_leaf_pages(page_rc, right_rc)?;
        } else {
            return Err(MyError::new(
                "BTreeTable::handle_erratic_leaf_page no left or right sibling",
            ));
        };

        return Ok(());
    }

    /// Handle the case when an internal page becomes less than half full due
    /// to deletions.
    ///
    /// If one of its siblings has extra entries, redistribute those entries.
    /// Otherwise merge with one of the siblings. Update pointers as needed.
    ///
    /// # Arguments
    ///
    /// - page_rc - the erratic internal page to be handled
    fn handle_erratic_internal_page(
        &self,
        page_rc: Rc<RefCell<BTreeInternalPage>>,
    ) -> Result<(), MyError> {
        if page_rc.borrow().get_parent_pid().category
            == PageCategory::RootPointer
        {
            return Ok(());
        }

        let left_pid = page_rc.borrow().get_left_pid();
        let right_pid = page_rc.borrow().get_right_pid();
        if let Some(left_pid) = left_pid {
            let left_rc =
                BufferPool::global().get_internal_page(&left_pid).unwrap();
            self.balancing_two_internal_pages(left_rc, page_rc)?;
        } else if let Some(right_pid) = right_pid {
            let right_rc =
                BufferPool::global().get_internal_page(&right_pid).unwrap();
            self.balancing_two_internal_pages(page_rc, right_rc)?;
        } else {
            panic!("Cannot find the left/right sibling of the page");
        }

        Ok(())
    }

    fn set_parent_pid(
        &self,
        child_pid: &BTreePageID,
        parent_pid: &BTreePageID,
    ) {
        match child_pid.category {
            PageCategory::Leaf => {
                let child_rc =
                    BufferPool::global().get_leaf_page(child_pid).unwrap();
                child_rc.borrow_mut().set_parent_pid(&parent_pid);
            }
            PageCategory::Internal => {
                let child_rc =
                    BufferPool::global().get_internal_page(child_pid).unwrap();
                child_rc.borrow_mut().set_parent_pid(&parent_pid);
            }
            _ => panic!("Invalid page category"),
        }
    }

    /// # Arguments
    ///
    /// - parent_entry - the entry in the parent corresponding to the left and
    ///   right
    fn merge_internal_page(
        &self,
        left_rc: Rc<RefCell<BTreeInternalPage>>,
        right_rc: Rc<RefCell<BTreeInternalPage>>,
        parent_rc: Rc<RefCell<BTreeInternalPage>>,
        parent_entry: &Entry,
    ) -> Result<(), MyError> {
        // hold left_rc and right_rc
        {
            let mut left = left_rc.borrow_mut();
            let mut right = right_rc.borrow_mut();

            // stage 1: pull down the edge entry from parent and insert it into
            // target page
            let edge_entry = Entry::new(
                parent_entry.get_key(),
                &left.get_last_child_pid(),
                &right.get_first_child_pid(),
            );
            self.set_parent_pid(&right.get_first_child_pid(), &left.get_pid());
            left.insert_entry(&edge_entry)?;

            // stage 2: move the entries from the one page to the other
            let mut deleted_indexes = Vec::new();
            let iter = BTreeInternalPageIterator::new(&right);
            for e in iter {
                left.insert_entry(&e)?;
                self.set_parent_pid(&e.get_right_child(), &left.get_pid());
                deleted_indexes.push(e.get_record_id());
            }
            for i in deleted_indexes {
                right.delete_key_and_right_child(i);
            }

            // stage 3: set the right as empty
            self.set_empty_page(&right.get_pid());
        }
        // release left_rc and right_rc

        // stage 4: update the entry in parent which points to the left and
        // right
        self.delete_parent_entry(left_rc, parent_rc, parent_entry)?;

        Ok(())
    }

    /// # Arguments
    ///
    /// - entry - the entry in the parent corresponding to the left_child and
    ///   right_child
    fn merge_leaf_page(
        &self,
        left_rc: Rc<RefCell<BTreeLeafPage>>,
        right_rc: Rc<RefCell<BTreeLeafPage>>,
        parent_rc: Rc<RefCell<BTreeInternalPage>>,
        entry: &Entry,
    ) -> Result<(), MyError> {
        // hold the left and right page
        {
            let mut left = left_rc.borrow_mut();
            let mut right = right_rc.borrow_mut();

            // stage 1: move the tuples from right to left
            let mut it = BTreeLeafPageIterator::new(&right);
            let mut deleted = Vec::new();
            for t in it.by_ref() {
                left.insert_tuple(&t);
                deleted.push(t.get_slot_number());
            }
            for slot in deleted {
                right.delete_tuple(slot);
            }

            // stage 2: update sibling pointers

            // set the right pointer of the left page to the right page's right
            // pointer
            left.set_right_pid(right.get_right_pid());

            // set the left pointer for the newer right page
            if let Some(newer_right_pid) = right.get_right_pid() {
                let newer_right_rc = BufferPool::global()
                    .get_leaf_page(&newer_right_pid)
                    .unwrap();
                newer_right_rc
                    .borrow_mut()
                    .set_left_pid(Some(left.get_pid()));
            }

            // stage 4: set the right page as empty
            self.set_empty_page(&right.get_pid());
        }

        // stage 5: release the left and right page
        self.delete_parent_entry(left_rc, parent_rc, entry)?;

        Ok(())
    }

    /// Method to encapsulate the process of deleting an entry (specifically
    /// the key and right child) from a parent node.
    ///
    /// If the parent becomes empty (no keys remaining), that indicates that
    /// it was the root node and should be replaced by its one remaining
    /// child.
    ///
    /// Otherwise, if it gets below minimum occupancy for non-root internal
    /// nodes, it should steal from one of its siblings or merge with a sibling.
    ///
    /// # Arguments
    ///
    /// - reserved_child    - the child reserved after the key and another child
    ///   are deleted
    /// - page              - the parent containing the entry to be deleted
    /// - entry             - the entry to be deleted
    /// - delete_left_child - which child of the entry should be deleted
    fn delete_parent_entry<T>(
        &self,
        left_rc: Rc<RefCell<T>>,
        parent_rc: Rc<RefCell<BTreeInternalPage>>,
        entry: &Entry,
    ) -> Result<(), MyError>
    where
        T: DerefMut<Target = dyn BTreePage>,
    {
        // hold the parent and left page
        {
            let mut parent = parent_rc.borrow_mut();
            let mut left = left_rc.borrow_mut();

            // stage 1: delete the corresponding entry in the parent page
            parent.delete_key_and_right_child(entry.get_record_id());

            // stage 2: handle the parent page according to the following cases
            // case 1: parent is empty, then the left child is now the new root
            if parent.entries_count() == 0 {
                let root_ptr_page_rc = self.get_root_ptr_page();

                // hold the root pointer page
                {
                    let mut root_ptr_page = root_ptr_page_rc.borrow_mut();
                    left.set_parent_pid(&root_ptr_page.get_pid());
                    root_ptr_page.set_root_pid(&left.get_pid());
                }
                // release the root pointer page

                // release the page for reuse
                self.set_empty_page(&parent.get_pid());
                return Ok(());
            }

            // case 2: parent is stable, return directly
            if parent.stable() {
                return Ok(());
            }
        }
        // release the parent and left page

        // case 3: parent is unstable (erratic), handle it
        self.handle_erratic_internal_page(parent_rc)?;
        Ok(())
    }

    /// Mark a page in this BTreeTable as empty. Find the corresponding header
    /// page (create it if needed), and mark the corresponding slot in the
    /// header page as empty.
    fn set_empty_page(&self, pid: &BTreePageID) {
        BufferPool::global().discard_page(pid);

        let root_ptr_rc = self.get_root_ptr_page();
        let header_rc: Rc<RefCell<BTreeHeaderPage>>;

        // let mut root_ptr = root_ptr_rc.borrow_mut();
        match root_ptr_rc.borrow().get_header_pid() {
            Some(header_pid) => {
                header_rc =
                    BufferPool::global().get_header_page(&header_pid).unwrap();
            }
            None => {
                // if there are no header pages, create the first header
                // page and update the header pointer
                // in the BTreeRootPtrPage
                header_rc = self.get_empty_header_page();
            }
        }

        root_ptr_rc
            .borrow_mut()
            .set_header_pid(&header_rc.borrow().get_pid());

        // borrow of header_rc start here
        {
            let mut header = header_rc.borrow_mut();
            let slot_index = pid.page_index % header.get_slots_count();
            header.mark_slot_status(slot_index, false);
        }
        // borrow of header_rc end here
    }

    /// Balancing two internal pages according the situation:
    ///
    /// 1.  Merge the two pages if the count of entries in the two pages is
    /// less than the maximum capacity of a single page.
    ///
    /// 2.  Otherwise, steal entries from the sibling and copy them to the
    /// given page so that both pages are at least half full.
    ///
    /// Keys can be thought of as rotating through the parent entry, so
    /// the original key in the parent is "pulled down" to the erratic
    /// page, and the last key in the sibling page is "pushed up" to
    /// the parent.  Update parent pointers as needed.
    fn balancing_two_internal_pages(
        &self,
        left_rc: Rc<RefCell<BTreeInternalPage>>,
        right_rc: Rc<RefCell<BTreeInternalPage>>,
    ) -> Result<(), MyError> {
        let parent_rc = BufferPool::global()
            .get_internal_page(&left_rc.borrow().get_parent_pid())
            .unwrap();
        let mut parent_entry = parent_rc
            .borrow()
            .get_entry_by_children(
                &left_rc.borrow().get_pid(),
                &right_rc.borrow().get_pid(),
            )
            .unwrap();

        let left_entries = left_rc.borrow().entries_count();
        let right_entries = right_rc.borrow().entries_count();
        if left_entries + right_entries < left_rc.borrow().get_max_capacity() {
            // if the two pages can be merged, merge them
            return self.merge_internal_page(
                left_rc,
                right_rc,
                parent_rc,
                &parent_entry,
            );
        }

        // if there aren't any entries to move, return immediately
        let move_count = (left_entries + right_entries) / 2
            - cmp::min(left_entries, right_entries);
        if move_count == 0 {
            return Ok(());
        }

        let mut middle_key = parent_entry.get_key();

        // hold the left and right page
        {
            let mut left = left_rc.borrow_mut();
            let mut right = right_rc.borrow_mut();

            if left_entries < right_entries {
                // The edge child of the destination page.
                let edge_child_pid = left.get_last_child_pid();
                let iter = BTreeInternalPageIterator::new(&right);
                let moved_records = self.move_entries(
                    left,
                    move_count,
                    iter,
                    &mut middle_key,
                    edge_child_pid,
                )?;
                for i in moved_records {
                    right.delete_key_and_left_child(i);
                }
            } else {
                let edge_child_pid = right.get_first_child_pid();
                let iter_b = BTreeInternalPageIterator::new(&left).rev();
                let moved_records = self.move_entries(
                    right,
                    move_count,
                    iter_b,
                    &mut middle_key,
                    edge_child_pid,
                )?;

                for i in moved_records {
                    left.delete_key_and_right_child(i);
                }
            }
        }
        // release the left and right page

        parent_entry.set_key(middle_key);
        parent_rc.borrow_mut().update_entry(&parent_entry);
        Ok(())
    }

    /// Arguments:
    /// * `middle_key`:
    ///
    ///     The key between the left and right pages.
    ///
    ///     This key is always larger than children in the left page and
    ///     smaller than children in the right page. It should be updated
    ///     each time an entry is moved from the left/right page to the
    ///     otherside.
    fn move_entries(
        &self,
        mut dest: impl DerefMut<Target = BTreeInternalPage>,
        // mut src: impl DerefMut<Target = BTreeInternalPage>,
        move_count: usize,
        src_iter: impl Iterator<Item = Entry>,
        middle_key: &mut IntField,
        mut edge_child_pid: BTreePageID,
    ) -> Result<Vec<usize>, MyError> {
        // Remember the entries for deletion later (cause we can't
        // modify the page while iterating though it)
        let mut moved_records = Vec::new();

        for e in src_iter.take(move_count) {
            // 1. delete the entry from the right page
            moved_records.push(e.get_record_id());

            let new_entry =
                Entry::new(*middle_key, &edge_child_pid, &e.get_left_child());
            debug!("balancing_two_internal_pages: new_entry = {:?}", new_entry,);

            // 1. insert entry
            dest.insert_entry(&new_entry)?;

            // 2. update parent id for the moved child
            debug!(
                "set parent pid: {:?} -> {:?}",
                e.get_left_child(),
                dest.get_pid()
            );
            self.set_parent_pid(&&e.get_left_child(), &dest.get_pid());

            // 4. update key and edge child for the next iteration
            *middle_key = e.get_key();
            edge_child_pid = e.get_left_child();
        }
        return Ok(moved_records);
    }

    // fn wtf<T>(&self, a: &mut T)
    // where
    //     T: Iterator<Item = Entry>,
    // {
    // }

    /// Steal tuples from a sibling and copy them to the given page so that both
    /// pages are at least half full.  Update the parent's entry so that the
    /// key matches the key field of the first tuple in the right-hand page.
    ///
    /// # Arguments
    ///
    /// - page           - the leaf page which is less than half full
    /// - sibling        - the sibling which has tuples to spare
    /// - parent         - the parent of the two leaf pages
    /// - entry          - the entry in the parent pointing to the two leaf
    ///   pages
    /// - is_right_sibling - whether the sibling is a right-sibling
    fn balancing_two_leaf_pages(
        &self,
        left_rc: Rc<RefCell<BTreeLeafPage>>,
        right_rc: Rc<RefCell<BTreeLeafPage>>,
    ) -> Result<(), MyError> {
        let parent_rc = BufferPool::global()
            .get_internal_page(&left_rc.borrow().get_parent_pid())
            .unwrap();
        let mut entry = parent_rc
            .borrow()
            .get_entry_by_children(
                &left_rc.borrow().get_pid(),
                &right_rc.borrow().get_pid(),
            )
            .unwrap();

        let left_tuples = left_rc.borrow().tuples_count();
        let right_tuples = right_rc.borrow().tuples_count();
        if left_tuples + right_tuples <= left_rc.borrow().get_slots_count() {
            // if the two pages can be merged, merge them
            return self.merge_leaf_page(left_rc, right_rc, parent_rc, &entry);
        }

        let move_count = (left_tuples + right_tuples) / 2
            - cmp::min(left_tuples, right_tuples);
        if move_count == 0 {
            return self.merge_leaf_page(left_rc, right_rc, parent_rc, &entry);
        }

        let mut key = entry.get_key();

        // hold left and right page
        {
            let mut left = left_rc.borrow_mut();
            let mut right = right_rc.borrow_mut();

            if left_tuples < right_tuples {
                let iter = BTreeLeafPageIterator::new(&right);
                let mut deleted_indexes = Vec::new();
                for tuple in iter.take(move_count) {
                    left.insert_tuple(&tuple);
                    deleted_indexes.push(tuple.get_slot_number());
                    key = tuple.get_field(self.key_field);
                }
                for i in deleted_indexes {
                    right.delete_tuple(i);
                }
            } else {
                let iter = BTreeLeafPageIterator::new(&left);
                let mut deleted_indexes = Vec::new();
                for tuple in iter.rev().take(move_count) {
                    right.insert_tuple(&tuple);
                    deleted_indexes.push(tuple.get_slot_number());
                    key = tuple.get_field(self.key_field);
                }
                for i in deleted_indexes {
                    left.delete_tuple(i);
                }
            }
        }
        // release left and right page

        entry.set_key(key);
        parent_rc.borrow_mut().update_entry(&entry);

        Ok(())
    }
}

impl BTreeTable {
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

    /// Recursive function which finds and locks the leaf page in
    /// the B+ tree corresponding to the left-most page possibly
    /// containing the key field f. It locks all internal nodes
    /// along the path to the leaf node with READ_ONLY permission,
    /// and locks the leaf node with permission perm.
    ///
    /// # Arguments
    ///
    /// tid  - the transaction id
    /// pid  - the current page being searched
    /// perm - the permissions with which to lock the leaf page
    /// f    - the field to search for
    ///
    /// # Return
    ///
    /// the left-most leaf page possibly containing the key field f
    fn find_leaf_page(
        &self,
        page_id: BTreePageID,
        search: SearchFor,
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
                        match search {
                            SearchFor::IntField(field) => {
                                if e.get_key() >= field {
                                    child_pid = Some(e.get_left_child());
                                    found = true;
                                    break;
                                }
                            }
                            SearchFor::LeftMost => {
                                child_pid = Some(e.get_left_child());
                                found = true;
                                break;
                            }
                            SearchFor::RightMost => {
                                child_pid = Some(e.get_right_child());
                                found = true;

                                // dont't break here, we need to find the
                                // rightmost entry
                            }
                        }
                        entry = Some(e);
                    }

                    if !found {
                        // if not found, search in right of the last entry
                        match entry {
                            Some(e) => {
                                child_pid = Some(e.get_right_child());
                            }
                            None => todo!(),
                        }
                    }
                }
                // borrow of page_rc end here

                // search child page recursively
                match child_pid {
                    Some(child_pid) => {
                        return self.find_leaf_page(child_pid, search);
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

    /// init file in necessary
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

    fn read_page(&self, _page_id: &BTreePageID) -> BTreeVirtualPage {
        todo!()
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

    fn get_empty_header_page(&self) -> Rc<RefCell<BTreeHeaderPage>> {
        // create the new page
        let page_index = self.get_empty_page_index();
        let page_id =
            BTreePageID::new(PageCategory::Header, self.table_id, page_index);
        let page = BTreeHeaderPage::new(&page_id);

        self.write_page_to_disk(&page_id);

        let page_rc = Rc::new(RefCell::new(page));

        BufferPool::global()
            .header_buffer
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
        return self.find_leaf_page(page_id, SearchFor::LeftMost);
    }

    pub fn get_last_page(&self) -> Rc<RefCell<BTreeLeafPage>> {
        let page_id = self.get_root_pid();
        return self.find_leaf_page(page_id, SearchFor::RightMost);
    }

    /// Get the root page pid.
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

    /// The count of pages in this BTreeFile
    ///
    /// (the ROOT_POINTER page is not included)
    pub fn pages_count(&self) -> usize {
        let file_size = self.file.borrow().metadata().unwrap().len() as usize;
        debug!(
            "file size: {}, page size: {}",
            file_size,
            BufferPool::get_page_size()
        );
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
                    let mut it = BTreeInternalPageIterator::new(&page);
                    child_pid = it.next_back().unwrap().get_right_child();
                }
                // borrow of page_rc end here
                self.get_last_tuple(&child_pid)
            }
            PageCategory::Leaf => {
                let page_rc = BufferPool::global().get_leaf_page(pid).unwrap();

                let page = page_rc.borrow();
                let mut it = BTreeLeafPageIterator::new(&page);
                it.next_back()
            }
            PageCategory::Header => todo!(),
        }
    }
}

/// debug methods
impl BTreeTable {
    /// Print the BTreeFile structure.
    ///
    /// # Arguments
    ///
    /// - `max_level` - the max level of the print
    ///     - 0: print the root pointer page
    ///     - 1: print the root pointer page and the root page (internal or
    ///       leaf)
    ///     - ...
    ///     - -1: print all pages
    pub fn draw_tree(&self, max_level: i32) {
        // return if the log level is not debug
        if env::var("RUST_LOG").unwrap_or_default() != "debug" {
            return;
        }

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
        let print_sibling = false;

        let mut prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_leaf_page(&pid).unwrap();

        let mut it = BTreeLeafPageIteratorRc::new(Rc::clone(&page_rc));
        let first_tuple = it.next().unwrap();

        let page = page_rc.borrow();
        let mut rit = BTreeLeafPageIterator::new(&page);
        let last_tuple = rit.next_back().unwrap();

        if print_sibling {
            println!(
                "{}├── leaf: {} ({} tuples) (left: {:?}, right: {:?})",
                prefix,
                page.get_pid(),
                page.tuples_count(),
                page.get_left_pid(),
                page.get_right_pid(),
            );
        } else {
            println!(
                "{}├── leaf: {} ({}/{} tuples)",
                prefix,
                page.get_pid(),
                page.tuples_count(),
                page.slot_count,
            );
        }

        prefix = "│   ".repeat(level + 1);
        println!("{}├── first tuple: {}", prefix, first_tuple);
        println!("{}└── last tuple:  {}", prefix, last_tuple);
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
                "{}├── internal: {} ({}/{} entries)",
                prefix,
                pid,
                page.entries_count(),
                page.get_max_capacity(),
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

    /// checks the integrity of the tree:
    /// - parent pointers.
    /// - sibling pointers.
    /// - range invariants.
    /// - record to page pointers.
    /// - occupancy invariants. (if enabled)
    ///
    /// panic on any error found.
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
        assert!(
            root_summary.left_ptr.is_none(),
            "left pointer is not none: {:?}",
            root_summary.left_ptr
        );
        assert!(
            root_summary.right_ptr.is_none(),
            "right pointer is not none: {:?}",
            root_summary.right_ptr,
        );
    }

    /// panic on any error found.
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
                    left_ptr: page.get_left_pid(),
                    right_ptr: page.get_right_pid(),

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
        assert_eq!(
            self.right_ptr, right.left_most_pid,
            "depth: {}, left_ptr: {:?}, right_ptr: {:?}",
            self.depth, self.right_ptr, right.left_most_pid
        );
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
    last_page_rc: Rc<RefCell<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,
    last_page_it: BTreeLeafPageIteratorRc,
}

impl BTreeTableIterator {
    pub fn new(table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page();
        let last_page_rc = table.get_last_page();

        Self {
            page_rc: Rc::clone(&page_rc),
            last_page_rc: Rc::clone(&last_page_rc),
            page_it: BTreeLeafPageIteratorRc::new(Rc::clone(&page_rc)),
            last_page_it: BTreeLeafPageIteratorRc::new(Rc::clone(
                &last_page_rc,
            )),
        }
    }
}

impl Iterator for BTreeTableIterator {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.page_it.next();
        if !v.is_none() {
            return v;
        }

        let right = self.page_rc.borrow().get_right_pid();
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

impl DoubleEndedIterator for BTreeTableIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let v = self.last_page_it.next_back();
        if !v.is_none() {
            return v;
        }

        let left = self.last_page_rc.borrow().get_left_pid();
        match left {
            Some(left) => {
                let sibling_rc =
                    BufferPool::global().get_leaf_page(&left).unwrap();
                let page_it =
                    BTreeLeafPageIteratorRc::new(Rc::clone(&sibling_rc));

                self.last_page_rc = Rc::clone(&sibling_rc);
                self.last_page_it = page_it;
                return self.last_page_it.next_back();
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
        let start_rc: Rc<RefCell<BTreeLeafPage>>;

        let root_pid = table.get_root_pid();

        match index_predicate.op {
            Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
                start_rc = table.find_leaf_page(
                    root_pid,
                    SearchFor::IntField(index_predicate.field),
                )
            }
            Op::LessThan | Op::LessThanOrEq => {
                start_rc = table.find_leaf_page(root_pid, SearchFor::LeftMost)
            }
            Op::Like => todo!(),
            Op::NotEquals => todo!(),
        }

        Self {
            current_page_rc: Rc::clone(&start_rc),
            page_it: BTreeLeafPageIteratorRc::new(Rc::clone(&start_rc)),
            predicate: index_predicate,
            key_field: table.key_field,
        }
    }
}

impl Iterator for BTreeTableSearchIterator {
    type Item = WrappedTuple;

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
                    let right =
                        (*self.current_page_rc).borrow().get_right_pid();
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
