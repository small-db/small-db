use core::fmt;
use std::{
    cmp,
    collections::hash_map::DefaultHasher,
    env,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Seek, SeekFrom, Write},
    ops::DerefMut,
    str,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, RwLock,
    },
    time::SystemTime,
    usize,
};

use log::debug;

use super::{
    buffer_pool::BufferPool,
    page::{
        empty_page_data, BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage,
        BTreeLeafPageIterator, BTreeLeafPageIteratorRc, BTreePageID,
        BTreeRootPointerPage, BTreeVirtualPage, Entry,
    },
    tuple::{Tuple, TupleScheme, WrappedTuple},
};
use crate::{
    btree::page::{
        BTreeBasePage, BTreeInternalPageIterator, BTreePage, PageCategory,
    },
    concurrent_status::{Lock, Permission},
    error::SimpleError,
    field::IntField,
    transaction::Transaction,
    types::ResultPod,
    utils::{lock_state, HandyRwLock},
};

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

    file: Mutex<File>,

    table_id: i32,

    /// the page index of the last page in the file
    ///
    /// The page index start from 0 and increase monotonically by 1, the page
    /// index of "root pointer" page is always 0.
    page_index: AtomicUsize,
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

        let f = Mutex::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .open(file_path)
                .unwrap(),
        );

        // let file_size = f.rl().metadata().unwrap().len() as usize;
        // debug!("btree initialized, file size: {}", file_size);

        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let table_id = hasher.finish() as i32;

        Self::file_init(f.lock().unwrap());

        Self {
            file_path: file_path.to_string(),
            key_field,
            tuple_scheme: row_scheme.clone(),
            file: f,
            table_id,

            // start from 1 (the root page)
            //
            // TODO: init it according to actual condition
            page_index: AtomicUsize::new(1),
        }
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    pub fn get_tuple_scheme(&self) -> TupleScheme {
        self.tuple_scheme.clone()
    }

    pub fn insert_tuple_auto_tx(
        &self,
        tuple: &Tuple,
    ) -> Result<(), SimpleError> {
        let tx = Transaction::new();
        self.insert_tuple(&tx, &tuple)?;
        tx.commit();
        return Ok(());
    }

    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    /// May cause pages to split if the page where tuple belongs is full.
    pub fn insert_tuple(
        &self,
        tx: &Transaction,
        tuple: &Tuple,
    ) -> Result<(), SimpleError> {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid();

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let field = tuple.get_field(self.key_field);
        let mut leaf_rc = self.find_leaf_page(
            tx,
            Permission::ReadWrite,
            root_pid,
            SearchFor::IntField(field),
        );

        if leaf_rc.rl().empty_slots_count() == 0 {
            leaf_rc = self.split_leaf_page(
                tx,
                leaf_rc,
                tuple.get_field(self.key_field),
            )?;
        }
        leaf_rc.wl().insert_tuple(&tuple);
        return Ok(());
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
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeLeafPage>>,
        field: IntField,
    ) -> ResultPod<BTreeLeafPage> {
        let new_sibling_rc = self.get_empty_leaf_page();
        let parent_pid: BTreePageID;
        let key: IntField;

        // borrow of new_sibling_rc start here
        // borrow of page_rc start here
        {
            let mut new_sibling = new_sibling_rc.wl();
            let mut page = page_rc.wl();
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
        let parent_rc = self.get_parent_with_empty_slots(tx, parent_pid, field);

        // borrow of parent_rc start here
        // borrow of page_rc start here
        // borrow of new_sibling_rc start here
        {
            let mut parent = parent_rc.wl();
            let mut page = page_rc.wl();
            let mut new_sibling = new_sibling_rc.wl();
            let mut entry =
                Entry::new(key, &page.get_pid(), &new_sibling.get_pid());

            debug!(
                "split start, page: {}, lock status: {}, new_sibling: {}, lock status: {}, parent: {}, lock status: {}",
                page.get_pid(),
                lock_state(page_rc.clone()),
                new_sibling.get_pid(),
                lock_state(new_sibling_rc.clone()),
                parent.get_pid(),
                lock_state(parent_rc.clone()),
            );

            parent.insert_entry(&mut entry)?;

            // set left pointer for the old right sibling
            if let Some(old_right_pid) = page.get_right_pid() {
                let old_right_rc = BufferPool::global()
                    .get_leaf_page(tx, Permission::ReadWrite, &old_right_pid)
                    .unwrap();
                old_right_rc.wl().set_left_pid(Some(new_sibling.get_pid()));
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
            Ok(new_sibling_rc)
        } else {
            Ok(page_rc)
        }
    }

    pub fn get_empty_page_index(&self) -> usize {
        let root_ptr_rc = self.get_root_ptr_page();
        // borrow of root_ptr_rc start here
        {
            let root_ptr = root_ptr_rc.rl();
            let header_pid = root_ptr.get_header_pid();
            if let Some(header_pid) = header_pid {
                let header_rc =
                    BufferPool::global().get_header_page(&header_pid).unwrap();
                // borrow of header_rc start here
                {
                    let header = header_rc.rl();
                    if let Some(i) = header.get_empty_slot() {
                        return i;
                    }
                }
            }
        }
        // borrow of root_ptr_rc end here

        let index = self.page_index.fetch_add(1, Ordering::Relaxed) + 1;
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
        tx: &Transaction,
        parent_id: BTreePageID,
        field: IntField,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        // create a parent node if necessary
        // this will be the new root of the tree
        match parent_id.category {
            PageCategory::RootPointer => {
                let new_parent_rc = self.get_empty_interanl_page();

                // borrow of new_parent_rc start here
                {
                    let new_parent = new_parent_rc.wl();

                    // update the root pointer
                    let page_id = BTreePageID::new(
                        PageCategory::RootPointer,
                        self.table_id,
                        0,
                    );
                    let root_pointer_page = BufferPool::global()
                        .get_root_pointer_page(&page_id)
                        .unwrap();

                    root_pointer_page.wl().set_root_pid(&new_parent.get_pid());
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
                    empty_slots_count = parent_rc.rl().empty_slots_count();
                }
                // borrow of parent_rc end here

                if empty_slots_count > 0 {
                    return parent_rc;
                } else {
                    // split upper parent
                    return self.split_internal_page(tx, parent_rc, field);
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
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeInternalPage>>,
        field: IntField,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        let sibling_rc = self.get_empty_interanl_page();
        let key: IntField;
        let mut parent_pid: BTreePageID;
        let mut new_entry: Entry;

        // borrow of sibling_rc start here
        // borrow of page_rc start here
        {
            let mut sibling = sibling_rc.wl();
            let mut page = page_rc.wl();

            parent_pid = page.get_parent_pid();

            if parent_pid.category == PageCategory::RootPointer {
                // create new parent page if the parent page is root pointer
                // page.
                let parent_rc = self.get_empty_interanl_page();
                parent_pid = parent_rc.rl().get_pid();

                // update the root pointer
                let root_pointer_pid = BTreePageID::new(
                    PageCategory::RootPointer,
                    self.table_id,
                    0,
                );
                let root_pointer_page = BufferPool::global()
                    .get_root_pointer_page(&root_pointer_pid)
                    .unwrap();
                root_pointer_page.wl().set_root_pid(&parent_pid);
            }

            let enties_count = page.entries_count();
            let move_entries_count = enties_count / 2;

            let mut delete_indexes: Vec<usize> = Vec::new();
            let mut it = BTreeInternalPageIterator::new(&page);
            for e in it.by_ref().rev().take(move_entries_count) {
                delete_indexes.push(e.get_record_id());
                sibling.insert_entry(&e).unwrap();

                // set parent id for right child
                let right_pid = e.get_right_child();
                Self::set_parent(tx, &right_pid, &sibling.get_pid());
            }

            let middle_entry = it.next_back().unwrap();

            // also delete the middle entry
            delete_indexes.push(middle_entry.get_record_id());
            for i in delete_indexes {
                page.delete_key_and_right_child(i);
            }

            // set parent id for right child to the middle entry
            Self::set_parent(
                tx,
                &middle_entry.get_right_child(),
                &sibling.get_pid(),
            );

            key = middle_entry.get_key();
            new_entry = Entry::new(key, &page.get_pid(), &sibling.get_pid());
        }
        // borrow of sibling_rc end here
        // borrow of page_rc end here

        let parent_rc = self.get_parent_with_empty_slots(tx, parent_pid, field);
        parent_pid = parent_rc.rl().get_pid();
        page_rc.wl().set_parent_pid(&parent_pid);
        sibling_rc.wl().set_parent_pid(&parent_pid);

        // borrow of parent_rc start here
        {
            let mut parent = parent_rc.wl();
            parent.insert_entry(&mut new_entry).unwrap();
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
    pub fn delete_tuple_auto_tx(
        &self,
        tuple: &WrappedTuple,
    ) -> Result<(), SimpleError> {
        let tx = Transaction::new();
        self.delete_tuple(&tx, &tuple)?;
        tx.commit();
        return Ok(());
    }

    /// Delete a tuple from this BTreeFile.
    ///
    /// May cause pages to merge or redistribute entries/tuples if the pages
    /// become less than half full.
    pub fn delete_tuple(
        &self,
        tx: &Transaction,
        tuple: &WrappedTuple,
    ) -> Result<(), SimpleError> {
        let pid = tuple.get_pid();
        let leaf_rc = BufferPool::global()
            .get_leaf_page(tx, Permission::ReadWrite, &pid)
            .unwrap();

        // hold the leaf page
        {
            let mut leaf = leaf_rc.wl();
            leaf.delete_tuple(tuple.get_slot_number());
        }
        // release the leaf page

        if leaf_rc.rl().stable() {
            return Ok(());
        } else {
            return self.handle_erratic_leaf_page(tx, leaf_rc);
        }
    }

    /// Handle the case when a leaf page becomes less than half full due to
    /// deletions.
    ///
    /// If one of its siblings has extra tuples, redistribute those tuples.
    /// Otherwise merge with one of the siblings. Update pointers as needed.
    fn handle_erratic_leaf_page(
        &self,
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeLeafPage>>,
    ) -> Result<(), SimpleError> {
        if page_rc.rl().get_parent_pid().category == PageCategory::RootPointer {
            return Ok(());
        }

        let left_pid = page_rc.rl().get_left_pid();
        let right_pid = page_rc.rl().get_right_pid();

        if let Some(left_pid) = left_pid {
            let left_rc = BufferPool::global()
                .get_leaf_page(tx, Permission::ReadWrite, &left_pid)
                .unwrap();
            self.balancing_two_leaf_pages(tx, left_rc, page_rc)?;
        } else if let Some(right_pid) = right_pid {
            let right_rc = BufferPool::global()
                .get_leaf_page(tx, Permission::ReadWrite, &right_pid)
                .unwrap();
            self.balancing_two_leaf_pages(tx, page_rc, right_rc)?;
        } else {
            return Err(SimpleError::new(
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
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeInternalPage>>,
    ) -> Result<(), SimpleError> {
        if page_rc.rl().get_parent_pid().category == PageCategory::RootPointer {
            return Ok(());
        }

        let left_pid = page_rc.rl().get_left_pid();
        let right_pid = page_rc.rl().get_right_pid();
        if let Some(left_pid) = left_pid {
            let left_rc =
                BufferPool::global().get_internal_page(&left_pid).unwrap();
            self.balancing_two_internal_pages(tx, left_rc, page_rc)?;
        } else if let Some(right_pid) = right_pid {
            let right_rc =
                BufferPool::global().get_internal_page(&right_pid).unwrap();
            self.balancing_two_internal_pages(tx, page_rc, right_rc)?;
        } else {
            panic!("Cannot find the left/right sibling of the page");
        }

        Ok(())
    }

    fn set_parent_pid(
        &self,
        tx: &Transaction,
        child_pid: &BTreePageID,
        parent_pid: &BTreePageID,
    ) {
        match child_pid.category {
            PageCategory::Leaf => {
                let child_rc = BufferPool::global()
                    .get_leaf_page(tx, Permission::ReadWrite, child_pid)
                    .unwrap();
                child_rc.wl().set_parent_pid(&parent_pid);
            }
            PageCategory::Internal => {
                let child_rc =
                    BufferPool::global().get_internal_page(child_pid).unwrap();
                child_rc.wl().set_parent_pid(&parent_pid);
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
        tx: &Transaction,
        left_rc: Arc<RwLock<BTreeInternalPage>>,
        right_rc: Arc<RwLock<BTreeInternalPage>>,
        parent_rc: Arc<RwLock<BTreeInternalPage>>,
        parent_entry: &Entry,
    ) -> Result<(), SimpleError> {
        // hold left_rc and right_rc
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

            // stage 1: pull down the edge entry from parent and insert it into
            // target page
            let edge_entry = Entry::new(
                parent_entry.get_key(),
                &left.get_last_child_pid(),
                &right.get_first_child_pid(),
            );
            self.set_parent_pid(
                tx,
                &right.get_first_child_pid(),
                &left.get_pid(),
            );
            left.insert_entry(&edge_entry)?;

            // stage 2: move the entries from the one page to the other
            let mut deleted_indexes = Vec::new();
            let iter = BTreeInternalPageIterator::new(&right);
            for e in iter {
                left.insert_entry(&e)?;
                self.set_parent_pid(tx, &e.get_right_child(), &left.get_pid());
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
        self.delete_parent_entry(tx, left_rc, parent_rc, parent_entry)?;

        Ok(())
    }

    /// # Arguments
    ///
    /// - entry - the entry in the parent corresponding to the left_child and
    ///   right_child
    fn merge_leaf_page(
        &self,
        tx: &Transaction,
        left_rc: Arc<RwLock<BTreeLeafPage>>,
        right_rc: Arc<RwLock<BTreeLeafPage>>,
        parent_rc: Arc<RwLock<BTreeInternalPage>>,
        entry: &Entry,
    ) -> Result<(), SimpleError> {
        // hold the left and right page
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

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
                    .get_leaf_page(tx, Permission::ReadWrite, &newer_right_pid)
                    .unwrap();
                newer_right_rc.wl().set_left_pid(Some(left.get_pid()));
            }

            // stage 4: set the right page as empty
            self.set_empty_page(&right.get_pid());
        }

        // stage 5: release the left and right page
        self.delete_parent_entry(tx, left_rc, parent_rc, entry)?;

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
    fn delete_parent_entry<PAGE: BTreePage>(
        &self,
        tx: &Transaction,
        left_rc: Arc<RwLock<PAGE>>,
        parent_rc: Arc<RwLock<BTreeInternalPage>>,
        entry: &Entry,
    ) -> Result<(), SimpleError> {
        // hold the parent and left page
        {
            let mut parent = parent_rc.wl();
            let mut left = left_rc.wl();

            // stage 1: delete the corresponding entry in the parent page
            parent.delete_key_and_right_child(entry.get_record_id());

            // stage 2: handle the parent page according to the following cases
            // case 1: parent is empty, then the left child is now the new root
            if parent.entries_count() == 0 {
                let root_ptr_page_rc = self.get_root_ptr_page();

                // hold the root pointer page
                {
                    let mut root_ptr_page = root_ptr_page_rc.wl();
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
        self.handle_erratic_internal_page(tx, parent_rc)?;
        Ok(())
    }

    /// Mark a page in this BTreeTable as empty. Find the corresponding header
    /// page (create it if needed), and mark the corresponding slot in the
    /// header page as empty.
    fn set_empty_page(&self, pid: &BTreePageID) {
        BufferPool::global().discard_page(pid);

        let root_ptr_rc = self.get_root_ptr_page();
        let header_rc: Arc<RwLock<BTreeHeaderPage>>;

        // let mut root_ptr = root_ptr_rc.wl();
        match root_ptr_rc.rl().get_header_pid() {
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

        root_ptr_rc.wl().set_header_pid(&header_rc.rl().get_pid());

        // borrow of header_rc start here
        {
            let mut header = header_rc.wl();
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
        tx: &Transaction,
        left_rc: Arc<RwLock<BTreeInternalPage>>,
        right_rc: Arc<RwLock<BTreeInternalPage>>,
    ) -> Result<(), SimpleError> {
        let parent_rc = BufferPool::global()
            .get_internal_page(&left_rc.rl().get_parent_pid())
            .unwrap();
        let mut parent_entry = parent_rc
            .rl()
            .get_entry_by_children(
                &left_rc.rl().get_pid(),
                &right_rc.rl().get_pid(),
            )
            .unwrap();

        let left_entries = left_rc.rl().entries_count();
        let right_entries = right_rc.rl().entries_count();
        if left_entries + right_entries < left_rc.rl().get_max_capacity() {
            // if the two pages can be merged, merge them
            return self.merge_internal_page(
                tx,
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
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

            if left_entries < right_entries {
                // The edge child of the destination page.
                let edge_child_pid = left.get_last_child_pid();

                let right_iter = BTreeInternalPageIterator::new(&right);

                let moved_records = self.move_entries(
                    tx,
                    right_iter,
                    left,
                    move_count,
                    &mut middle_key,
                    edge_child_pid,
                    |edge_pid: BTreePageID, _e: &Entry| edge_pid,
                    |_edge_pid: BTreePageID, e: &Entry| e.get_left_child(),
                    |e: &Entry| e.get_left_child(),
                )?;

                for i in moved_records {
                    right.delete_key_and_left_child(i);
                }
            } else {
                // The edge child of the destination page.
                let edge_child_pid = right.get_first_child_pid();

                let left_iter = BTreeInternalPageIterator::new(&left).rev();

                let moved_records = self.move_entries(
                    tx,
                    left_iter,
                    right,
                    move_count,
                    &mut middle_key,
                    edge_child_pid,
                    |_edge_pid: BTreePageID, e: &Entry| e.get_right_child(),
                    |edge_pid: BTreePageID, _e: &Entry| edge_pid,
                    |e: &Entry| e.get_right_child(),
                )?;

                for i in moved_records {
                    left.delete_key_and_right_child(i);
                }
            }
        }
        // release the left and right page

        parent_entry.set_key(middle_key);
        parent_rc.wl().update_entry(&parent_entry);
        Ok(())
    }

    /// # Arguments:
    ///
    /// * `middle_key`: The key between the left and right pages. This key is
    ///   always larger than children in the left page and smaller than children
    ///   in the right page. It should be updated each time an entry is moved
    ///   from the left/right page to the otherside.
    ///
    /// * `edge_child_pid`: The edge child of the destination page.
    ///
    /// * `fn_get_edge_left_child`: A function to get the left child of the new
    ///   entry, the first argument is the edge child of the destination page,
    ///   the second argument is the current entry of the source page
    ///   (iterator).
    ///
    /// * `fn_get_edge_right_child`: Same as `fn_get_edge_left_child`, but for
    ///   the right child of the new entry.
    ///
    /// * `fn_get_moved_child`: A function to get the moved child page, the
    ///   argument is the current entry of the source page (iterator).
    ///
    /// Return:
    /// * The index of the moved entries in the source page.
    fn move_entries(
        &self,
        tx: &Transaction,
        src_iter: impl Iterator<Item = Entry>,
        mut dest: impl DerefMut<Target = BTreeInternalPage>,
        move_count: usize,
        middle_key: &mut IntField,
        mut edge_child_pid: BTreePageID,
        fn_get_edge_left_child: impl Fn(BTreePageID, &Entry) -> BTreePageID,
        fn_get_edge_right_child: impl Fn(BTreePageID, &Entry) -> BTreePageID,
        fn_get_moved_child: impl Fn(&Entry) -> BTreePageID,
    ) -> Result<Vec<usize>, SimpleError> {
        // Remember the entries for deletion later (cause we can't
        // modify the page while iterating though it)
        let mut moved_records = Vec::new();

        for e in src_iter.take(move_count) {
            // 1. delete the entry from the src page
            moved_records.push(e.get_record_id());

            // 2. insert new entry to dest page
            let new_entry = Entry::new(
                *middle_key,
                &fn_get_edge_left_child(edge_child_pid, &e),
                &fn_get_edge_right_child(edge_child_pid, &e),
            );
            dest.insert_entry(&new_entry)?;

            // 3. update parent id for the moved child
            self.set_parent_pid(tx, &fn_get_moved_child(&e), &dest.get_pid());

            // 4. update key and edge child for the next iteration
            *middle_key = e.get_key();
            edge_child_pid = fn_get_moved_child(&e);
        }
        return Ok(moved_records);
    }

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
        tx: &Transaction,
        left_rc: Arc<RwLock<BTreeLeafPage>>,
        right_rc: Arc<RwLock<BTreeLeafPage>>,
    ) -> Result<(), SimpleError> {
        let parent_rc = BufferPool::global()
            .get_internal_page(&left_rc.rl().get_parent_pid())
            .unwrap();
        let mut entry = parent_rc
            .rl()
            .get_entry_by_children(
                &left_rc.rl().get_pid(),
                &right_rc.rl().get_pid(),
            )
            .unwrap();

        let left_tuples = left_rc.rl().tuples_count();
        let right_tuples = right_rc.rl().tuples_count();
        if left_tuples + right_tuples <= left_rc.rl().get_slots_count() {
            // if the two pages can be merged, merge them
            return self
                .merge_leaf_page(tx, left_rc, right_rc, parent_rc, &entry);
        }

        let move_count = (left_tuples + right_tuples) / 2
            - cmp::min(left_tuples, right_tuples);
        if move_count == 0 {
            return self
                .merge_leaf_page(tx, left_rc, right_rc, parent_rc, &entry);
        }

        let mut key = entry.get_key();

        // hold left and right page
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

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
        parent_rc.wl().update_entry(&entry);

        Ok(())
    }
}

impl BTreeTable {
    /// Method to encapsulate the process of locking/fetching a page.  First the
    /// method checks the local cache ("dirtypages"), and if it can't find
    /// the requested page there, it fetches it from the buffer pool.
    /// It also adds pages to the dirtypages cache if they are fetched with
    /// read-write permission, since presumably they will soon be dirtied by
    /// this transaction.
    ///
    /// This method is needed to ensure that page updates are not lost if the
    /// same pages are accessed multiple times.
    ///
    /// reference:
    /// - https://sourcegraph.com/github.com/XiaochenCui/simple-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/simpledb/BTreeFile.java?L551&subtree=true
    pub fn get_page(&self) {}
}

impl BTreeTable {
    pub fn set_root_pid(&self, root_pid: &BTreePageID) {
        let root_pointer_pid =
            BTreePageID::new(PageCategory::RootPointer, self.table_id, 0);
        let root_pointer_rc = BufferPool::global()
            .get_root_pointer_page(&root_pointer_pid)
            .unwrap();
        root_pointer_rc.wl().set_root_pid(root_pid);
    }

    fn set_parent(
        tx: &Transaction,
        child_pid: &BTreePageID,
        parent_pid: &BTreePageID,
    ) {
        match child_pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let left_rc =
                    BufferPool::global().get_internal_page(&child_pid).unwrap();

                // borrow of left_rc start here
                {
                    let mut left = left_rc.wl();
                    left.set_parent_pid(&parent_pid);
                }
                // borrow of left_rc end here
            }
            PageCategory::Leaf => {
                let child_rc = BufferPool::global()
                    .get_leaf_page(tx, Permission::ReadWrite, &child_pid)
                    .unwrap();

                // borrow of left_rc start here
                {
                    let mut child = child_rc.wl();
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
        tx: &Transaction,
        perm: Permission,
        page_id: BTreePageID,
        search: SearchFor,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        match page_id.category {
            PageCategory::Leaf => {
                // get page and return directly
                return BufferPool::global()
                    .get_leaf_page(tx, perm, &page_id)
                    .unwrap();
            }
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(&page_id).unwrap();
                let mut child_pid: Option<BTreePageID> = None;

                // borrow of page_rc start here
                {
                    let page = page_rc.rl();
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
                        return self.find_leaf_page(
                            tx,
                            Permission::ReadWrite,
                            child_pid,
                            search,
                        );
                    }
                    None => todo!(),
                }
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap()
    }

    /// init file in necessary
    fn file_init(mut file: impl DerefMut<Target = File>) {
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

    fn get_empty_leaf_page(&self) -> Arc<RwLock<BTreeLeafPage>> {
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

        let page_rc = Arc::new(RwLock::new(page));

        BufferPool::global()
            .leaf_buffer
            .insert(page_id, page_rc.clone());

        page_rc
    }

    fn get_empty_interanl_page(&self) -> Arc<RwLock<BTreeInternalPage>> {
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

        let page_rc = Arc::new(RwLock::new(page));

        BufferPool::global()
            .internal_buffer
            .insert(page_id, page_rc.clone());

        page_rc
    }

    fn get_empty_header_page(&self) -> Arc<RwLock<BTreeHeaderPage>> {
        // create the new page
        let page_index = self.get_empty_page_index();
        let page_id =
            BTreePageID::new(PageCategory::Header, self.table_id, page_index);
        let page = BTreeHeaderPage::new(&page_id);

        self.write_page_to_disk(&page_id);

        let page_rc = Arc::new(RwLock::new(page));

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

    pub fn get_first_page(&self) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid();
        let tx = Transaction::new();
        return self.find_leaf_page(
            &tx,
            Permission::ReadWrite,
            page_id,
            SearchFor::LeftMost,
        );
    }

    pub fn get_last_page(&self) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid();
        let tx = Transaction::new();
        return self.find_leaf_page(
            &tx,
            Permission::ReadWrite,
            page_id,
            SearchFor::RightMost,
        );
    }

    /// Get the root page pid.
    pub fn get_root_pid(&self) -> BTreePageID {
        let root_ptr_rc = self.get_root_ptr_page();
        let mut root_pid = root_ptr_rc.rl().get_root_pid();
        root_pid.table_id = self.get_id();
        root_pid
    }

    pub fn get_root_ptr_page(&self) -> Arc<RwLock<BTreeRootPointerPage>> {
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
        let file_size = self.get_file().metadata().unwrap().len() as usize;
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
        self.page_index.store(i, Ordering::Relaxed);
    }

    // get the last tuple under the internal/leaf page
    pub fn get_last_tuple(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
    ) -> Option<WrappedTuple> {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::global().get_internal_page(pid).unwrap();

                // borrow of page_rc start here
                let child_pid: BTreePageID;
                {
                    let page = page_rc.rl();
                    let mut it = BTreeInternalPageIterator::new(&page);
                    child_pid = it.next_back().unwrap().get_right_child();
                }
                // borrow of page_rc end here
                self.get_last_tuple(tx, &child_pid)
            }
            PageCategory::Leaf => {
                let page_rc = BufferPool::global()
                    .get_leaf_page(tx, Permission::ReadWrite, pid)
                    .unwrap();

                let page = page_rc.rl();
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

        let mut depiction = "".to_string();

        depiction.push_str("\n\n----- PRINT TREE STRUCTURE START -----\n\n");

        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        depiction.push_str(&format!("root pointer: {}\n", root_pointer_pid));

        let root_pid = self.get_root_pid();
        depiction.push_str(&self.draw_subtree(&root_pid, 0, max_level));

        depiction.push_str(&format!(
            "\n\n----- PRINT TREE STRUCTURE END   -----\n\n"
        ));

        debug!("{}", depiction);
    }

    fn draw_subtree(
        &self,
        pid: &BTreePageID,
        level: usize,
        max_level: i32,
    ) -> String {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                self.draw_internal_node(pid, level, max_level)
            }
            PageCategory::Leaf => self.draw_leaf_node(pid, level),
            PageCategory::Header => todo!(),
        }
    }

    fn draw_leaf_node(&self, pid: &BTreePageID, level: usize) -> String {
        let mut depiction = "".to_string();

        let print_sibling = false;

        let mut prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global()
            .get_leaf_page(&Transaction::new(), Permission::ReadOnly, &pid)
            .unwrap();
        let lock_state = lock_state(page_rc.clone());

        let mut it = BTreeLeafPageIteratorRc::new(Arc::clone(&page_rc));
        let first_tuple = it.next().unwrap();

        let page = page_rc.rl();
        let mut it = BTreeLeafPageIterator::new(&page);
        let last_tuple = it.next_back().unwrap();

        if print_sibling {
            depiction.push_str(&format!(
                "{}├── leaf: {} ({} tuples) (left: {:?}, right: {:?}) (lock state: {})\n",
                prefix,
                page.get_pid(),
                page.tuples_count(),
                page.get_left_pid(),
                page.get_right_pid(),
                lock_state,
            ));
        } else {
            depiction.push_str(&format!(
                "{}├── leaf: {} ({}/{} tuples) (lock state: {}\n",
                prefix,
                page.get_pid(),
                page.tuples_count(),
                page.slot_count,
                lock_state,
            ));
        }

        prefix = "│   ".repeat(level + 1);
        depiction
            .push_str(&format!("{}├── first tuple: {}\n", prefix, first_tuple));
        depiction
            .push_str(&format!("{}└── last tuple:  {}\n", prefix, last_tuple));

        return depiction;
    }

    fn draw_internal_node(
        &self,
        pid: &BTreePageID,
        level: usize,
        max_level: i32,
    ) -> String {
        let mut depiction = "".to_string();

        let prefix = "│   ".repeat(level);
        let page_rc = BufferPool::global().get_internal_page(&pid).unwrap();
        let lock_state = lock_state(page_rc.clone());

        // borrow of page_rc start here
        {
            let page = page_rc.rl();
            depiction.push_str(&format!(
                "{}├── internal: {} ({}/{} entries) (lock state: {})\n",
                prefix,
                pid,
                page.entries_count(),
                page.get_max_capacity(),
                lock_state,
            ));
            if max_level != -1 && level as i32 == max_level {
                return depiction;
            }
            let it = BTreeInternalPageIterator::new(&page);
            for (i, entry) in it.enumerate() {
                depiction.push_str(&self.draw_entry(
                    i,
                    &entry,
                    level + 1,
                    max_level,
                ));
            }
        }
        // borrow of page_rc end here

        return depiction;
    }

    fn draw_entry(
        &self,
        id: usize,
        entry: &Entry,
        level: usize,
        max_level: i32,
    ) -> String {
        let mut depiction = "".to_string();

        let prefix = "│   ".repeat(level);
        if id == 0 {
            depiction.push_str(&self.draw_subtree(
                &entry.get_left_child(),
                level + 1,
                max_level,
            ));
        }
        depiction.push_str(&format!(
            "{}├── key: {}\n",
            prefix,
            entry.get_key()
        ));
        depiction.push_str(&self.draw_subtree(
            &entry.get_right_child(),
            level + 1,
            max_level,
        ));

        return depiction;
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
        let root_pid = root_ptr_page.rl().get_root_pid();
        let root_summary = self.check_sub_tree(
            &root_pid,
            &root_ptr_page.rl().get_pid(),
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
                let page_rc = BufferPool::global()
                    .get_leaf_page(
                        &Transaction::new(),
                        Permission::ReadOnly,
                        &pid,
                    )
                    .unwrap();
                let page = page_rc.rl();
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
                let page = page_rc.rl();
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

pub struct BTreeTableIterator<'t> {
    tx: &'t Transaction,

    page_rc: Arc<RwLock<BTreeLeafPage>>,
    last_page_rc: Arc<RwLock<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,
    last_page_it: BTreeLeafPageIteratorRc,
}

impl<'t> BTreeTableIterator<'t> {
    pub fn new(tx: &'t Transaction, table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page();
        let last_page_rc = table.get_last_page();

        Self {
            tx,
            page_rc: Arc::clone(&page_rc),
            last_page_rc: Arc::clone(&last_page_rc),
            page_it: BTreeLeafPageIteratorRc::new(Arc::clone(&page_rc)),
            last_page_it: BTreeLeafPageIteratorRc::new(Arc::clone(
                &last_page_rc,
            )),
        }
    }
}

impl Iterator for BTreeTableIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.page_it.next();
        if !v.is_none() {
            return v;
        }

        let right = self.page_rc.rl().get_right_pid();
        match right {
            Some(right) => {
                let sibling_rc = BufferPool::global()
                    .get_leaf_page(&self.tx, Permission::ReadOnly, &right)
                    .unwrap();
                let page_it =
                    BTreeLeafPageIteratorRc::new(Arc::clone(&sibling_rc));

                self.page_rc = Arc::clone(&sibling_rc);
                self.page_it = page_it;
                return self.page_it.next();
            }
            None => {
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for BTreeTableIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let v = self.last_page_it.next_back();
        if !v.is_none() {
            return v;
        }

        let left = self.last_page_rc.rl().get_left_pid();
        match left {
            Some(left) => {
                let sibling_rc = BufferPool::global()
                    .get_leaf_page(self.tx, Permission::ReadOnly, &left)
                    .unwrap();
                let page_it =
                    BTreeLeafPageIteratorRc::new(Arc::clone(&sibling_rc));

                self.last_page_rc = Arc::clone(&sibling_rc);
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

pub struct BTreeTableSearchIterator<'t> {
    tx: &'t Transaction,

    current_page_rc: Arc<RwLock<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,
    predicate: Predicate,
    key_field: usize,
}

impl<'t> BTreeTableSearchIterator<'t> {
    pub fn new(
        tx: &'t Transaction,
        table: &BTreeTable,
        index_predicate: Predicate,
    ) -> Self {
        let start_rc: Arc<RwLock<BTreeLeafPage>>;
        let root_pid = table.get_root_pid();

        match index_predicate.op {
            Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
                start_rc = table.find_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    root_pid,
                    SearchFor::IntField(index_predicate.field),
                )
            }
            Op::LessThan | Op::LessThanOrEq => {
                start_rc = table.find_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    root_pid,
                    SearchFor::LeftMost,
                )
            }
            Op::Like => todo!(),
            Op::NotEquals => todo!(),
        }

        Self {
            tx,
            current_page_rc: Arc::clone(&start_rc),
            page_it: BTreeLeafPageIteratorRc::new(Arc::clone(&start_rc)),
            predicate: index_predicate,
            key_field: table.key_field,
        }
    }
}

impl Iterator for BTreeTableSearchIterator<'_> {
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
                    let right = (*self.current_page_rc).rl().get_right_pid();
                    match right {
                        Some(pid) => {
                            let rc = BufferPool::global()
                                .get_leaf_page(
                                    self.tx,
                                    Permission::ReadOnly,
                                    &pid,
                                )
                                .unwrap();
                            self.current_page_rc = Arc::clone(&rc);
                            self.page_it =
                                BTreeLeafPageIteratorRc::new(Arc::clone(&rc));
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
