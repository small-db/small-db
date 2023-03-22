use core::fmt;
use std::{
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Seek, SeekFrom, Write},
    ops::DerefMut,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard, RwLock,
    },
    time::SystemTime,
    usize,
};

use log::debug;

use super::BTreeTableIterator;
use crate::{
    btree::{
        page::{
            BTreeBasePage, BTreeHeaderPage, BTreeInternalPage,
            BTreeInternalPageIterator, BTreeLeafPage,
            BTreeLeafPageIterator, BTreeLeafPageIteratorRc,
            BTreePage, BTreePageID, BTreeRootPointerPage, Entry,
            PageCategory,
        },
        page_cache::PageCache,
    },
    concurrent_status::Permission,
    storage::{
        schema::Schema,
        tuple::{Cell, Tuple, WrappedTuple},
    },
    transaction::Transaction,
    types::{ResultPod, SmallResult},
    utils::{lock_state, HandyRwLock},
    Database,
};

pub enum SearchFor {
    Target(Cell),
    LeftMost,
    RightMost,
}

/// B+ Tree
pub struct BTreeTable {
    pub name: String,

    // the field which index is keyed on
    pub key_field: usize,

    // the tuple descriptor of tuples in the file
    pub tuple_scheme: Schema,

    file: Mutex<File>,

    table_id: u32,

    /// the page index of the last page in the file
    ///
    /// The page index start from 0 and increase monotonically by 1,
    /// the page index of "root pointer" page is always 0.
    page_index: AtomicU32,
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
            "<BTreeFile, file: {:?}, id: {}>",
            "empty", self.table_id
        )
    }
}

// init functions
impl BTreeTable {
    pub fn new(
        table_name: &str,
        key_field: usize,
        row_scheme: &Schema,
    ) -> Self {
        let db_path = Database::global().get_path();

        let table_path =
            db_path.join(table_name).with_extension("table");

        let f = Mutex::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(table_path)
                .unwrap(),
        );

        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let table_id = hasher.finish() as u32;

        Self::file_init(f.lock().unwrap(), table_id);

        Self {
            name: table_name.to_string(),
            key_field,
            tuple_scheme: row_scheme.clone(),
            file: f,
            table_id,

            // start from 1 (the root page)
            //
            // TODO: init it according to actual condition
            page_index: AtomicU32::new(1),
        }
    }
}

// normal read-only functions
impl BTreeTable {
    pub fn get_id(&self) -> u32 {
        self.table_id
    }

    pub fn get_tuple_scheme(&self) -> Schema {
        self.tuple_scheme.clone()
    }

    /// Calculate the number of tuples in the table. Require S_LOCK on
    /// all pages.
    pub fn tuples_count(&self) -> usize {
        let tx = Transaction::new();
        let count = BTreeTableIterator::new(&tx, self).count();
        tx.commit().unwrap();
        count
    }

    pub fn get_random_tuple(&self, _tx: &Transaction) -> Tuple {
        unimplemented!()
    }
}

// insert-related functions
impl BTreeTable {
    /// Insert a tuple into this BTreeFile, keeping the tuples in
    /// sorted order. May cause pages to split if the page where
    /// tuple belongs is full.
    pub fn insert_tuple(
        &self,
        tx: &Transaction,
        // buffer_pool: &mut BufferPool,
        tuple: &Tuple,
    ) -> SmallResult {
        // a read lock on the root pointer page and
        // use it to locate the root page
        let root_pid = self.get_root_pid(tx);

        // find and lock the left-most leaf page corresponding to
        // the key field, and split the leaf page if there are no
        // more slots available
        let field = tuple.get_cell(self.key_field);
        let mut leaf_rc = self.find_leaf_page(
            tx,
            Permission::ReadWrite,
            root_pid,
            &SearchFor::Target(field),
        );

        if leaf_rc.rl().empty_slots_count() == 0 {
            leaf_rc = self.split_leaf_page(
                tx,
                leaf_rc,
                tuple.get_cell(self.key_field),
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
    /// `field`: the key field of the tuple to be inserted after the
    /// split is complete. Necessary to know which of the two
    /// pages to return.
    pub fn split_leaf_page(
        &self,
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeLeafPage>>,
        field: Cell,
    ) -> ResultPod<BTreeLeafPage> {
        let new_sibling_rc = self.get_empty_leaf_page(tx);
        let parent_pid: BTreePageID;
        let key: Cell;

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
            key = it.next_back().unwrap().get_cell(self.key_field);

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
        let parent_rc =
            self.get_parent_with_empty_slots(tx, parent_pid, &field);

        // borrow of parent_rc start here
        // borrow of page_rc start here
        // borrow of new_sibling_rc start here
        {
            let mut parent = parent_rc.wl();
            let mut page = page_rc.wl();
            let mut new_sibling = new_sibling_rc.wl();
            let mut entry = Entry::new(
                &key,
                &page.get_pid(),
                &new_sibling.get_pid(),
            );

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
                let old_right_rc = Database::mut_page_cache()
                    .get_leaf_page(
                        tx,
                        Permission::ReadWrite,
                        &old_right_pid,
                    )
                    .unwrap();
                old_right_rc
                    .wl()
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
            Ok(new_sibling_rc)
        } else {
            Ok(page_rc)
        }
    }

    pub fn get_empty_page_index(&self, tx: &Transaction) -> u32 {
        let root_ptr_rc = self.get_root_ptr_page(tx);
        // borrow of root_ptr_rc start here
        {
            let root_ptr = root_ptr_rc.rl();
            let header_pid = root_ptr.get_header_pid();
            if let Some(header_pid) = header_pid {
                let header_rc = Database::mut_page_cache()
                    .get_header_page(
                        tx,
                        Permission::ReadOnly,
                        &header_pid,
                    )
                    .unwrap();
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

        let index =
            self.page_index.fetch_add(1, Ordering::Relaxed) + 1;
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
    /// `field`: the key field of the tuple to be inserted after the
    /// split is complete. Necessary to know which of the two
    /// pages to return. `parentId`: the id of the parent. May be
    /// an internal page or the RootPtr page
    fn get_parent_with_empty_slots(
        &self,
        tx: &Transaction,
        parent_id: BTreePageID,
        field: &Cell,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        // create a parent node if necessary
        // this will be the new root of the tree
        match parent_id.category {
            PageCategory::RootPointer => {
                let new_parent_rc = self.get_empty_interanl_page(tx);

                // update the root pointer
                self.set_root_pid(tx, &new_parent_rc.wl().get_pid());

                new_parent_rc
            }
            PageCategory::Internal => {
                let parent_rc = Database::mut_page_cache()
                    .get_internal_page(
                        tx,
                        Permission::ReadWrite,
                        &parent_id,
                    )
                    .unwrap();
                let empty_slots_count: usize;

                // borrow of parent_rc start here
                {
                    empty_slots_count =
                        parent_rc.rl().empty_slots_count();
                }
                // borrow of parent_rc end here

                if empty_slots_count > 0 {
                    return parent_rc;
                } else {
                    // split upper parent
                    return self
                        .split_internal_page(tx, parent_rc, field);
                }
            }
            _ => {
                todo!()
            }
        }
    }

    /// Split an internal page to make room for new entries and
    /// recursively split its parent page as needed to accommodate
    /// a new entry. The new entry for the parent should have a
    /// key matching the middle key in the original internal page
    /// being split (this key is "pushed up" to the parent).
    ///
    /// Make a right sibling page and move half of entries to it.
    ///
    /// The child pointers of the new parent entry should point to the
    /// two internal pages resulting from the split. Update parent
    /// pointers as needed.
    ///
    /// Return the internal page into which an entry with key field
    /// "field" should be inserted
    ///
    /// # Arguments
    /// `field`: the key field of the tuple to be inserted after the
    /// split is complete. Necessary to know which of the two
    /// pages to return.
    fn split_internal_page(
        &self,
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeInternalPage>>,
        field: &Cell,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        let sibling_rc = self.get_empty_interanl_page(tx);
        let key: Cell;
        let mut parent_pid: BTreePageID;
        let mut new_entry: Entry;

        // borrow of sibling_rc start here
        // borrow of page_rc start here
        {
            let mut sibling = sibling_rc.wl();
            let mut page = page_rc.wl();

            parent_pid = page.get_parent_pid();

            if parent_pid.category == PageCategory::RootPointer {
                // create new parent page if the parent page is root
                // pointer page.
                let parent_rc = self.get_empty_interanl_page(tx);
                parent_pid = parent_rc.rl().get_pid();

                // update the root pointer
                self.set_root_pid(tx, &parent_pid);
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
            new_entry =
                Entry::new(&key, &page.get_pid(), &sibling.get_pid());
        }
        // borrow of sibling_rc end here
        // borrow of page_rc end here

        let parent_rc =
            self.get_parent_with_empty_slots(tx, parent_pid, field);
        parent_pid = parent_rc.rl().get_pid();
        page_rc.wl().set_parent_pid(&parent_pid);
        sibling_rc.wl().set_parent_pid(&parent_pid);

        // borrow of parent_rc start here
        {
            let mut parent = parent_rc.wl();
            parent.insert_entry(&mut new_entry).unwrap();
        }
        // borrow of parent_rc end here

        if *field > key {
            sibling_rc
        } else {
            page_rc
        }
    }
}

impl BTreeTable {
    /// Method to encapsulate the process of locking/fetching a page.
    /// First the method checks the local cache ("dirtypages"),
    /// and if it can't find the requested page there, it fetches
    /// it from the buffer pool. It also adds pages to the
    /// dirtypages cache if they are fetched with read-write
    /// permission, since presumably they will soon be dirtied by
    /// this transaction.
    ///
    /// This method is needed to ensure that page updates are not lost
    /// if the same pages are accessed multiple times.
    ///
    /// reference:
    /// - https://sourcegraph.com/github.com/XiaochenCui/small-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/smalldb/BTreeFile.java?L551&subtree=true
    pub fn get_page(&self) {}
}

// Methods interacting with disk directly
impl BTreeTable {
    fn get_empty_leaf_page(
        &self,
        tx: &Transaction,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        // create the new page
        let page_index = self.get_empty_page_index(tx);
        let page_id = BTreePageID::new(
            PageCategory::Leaf,
            self.table_id,
            page_index,
        );
        let page = BTreeLeafPage::new(
            &page_id,
            &BTreeBasePage::empty_page_data(),
            &self.tuple_scheme,
            self.key_field,
        );

        self.write_empty_page_to_disk(&page_id);

        let page_rc = Arc::new(RwLock::new(page));
        // insert to buffer pool because it's a dirty page at this
        // time
        Database::mut_page_cache()
            .leaf_buffer
            .insert(page_id, page_rc.clone());
        page_rc
    }

    fn get_empty_interanl_page(
        &self,
        tx: &Transaction,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        // create the new page
        let page_index = self.get_empty_page_index(tx);
        let page_id = BTreePageID::new(
            PageCategory::Internal,
            self.table_id,
            page_index,
        );
        let page = BTreeInternalPage::new(
            &page_id,
            &BTreeBasePage::empty_page_data(),
            &self.tuple_scheme,
            self.key_field,
        );

        self.write_empty_page_to_disk(&page_id);

        let page_rc = Arc::new(RwLock::new(page));
        // insert to buffer pool because it's a dirty page at this
        // time
        Database::mut_page_cache()
            .internal_buffer
            .insert(page_id, page_rc.clone());
        page_rc
    }

    pub(super) fn get_empty_header_page(
        &self,
        tx: &Transaction,
    ) -> Arc<RwLock<BTreeHeaderPage>> {
        // create the new page
        let page_index = self.get_empty_page_index(tx);
        let page_id = BTreePageID::new(
            PageCategory::Header,
            self.table_id,
            page_index,
        );
        let page = BTreeHeaderPage::new(
            &page_id,
            &BTreeBasePage::empty_page_data(),
        );

        self.write_empty_page_to_disk(&page_id);

        let page_rc = Arc::new(RwLock::new(page));
        // insert to buffer pool because it's a dirty page at this
        // time
        Database::mut_page_cache()
            .header_buffer
            .insert(page_id, page_rc.clone());
        page_rc
    }

    pub fn write_empty_page_to_disk(&self, page_id: &BTreePageID) {
        self.write_page_to_disk(
            page_id,
            &BTreeBasePage::empty_page_data(),
        )
    }

    pub fn write_page_to_disk(
        &self,
        page_id: &BTreePageID,
        data: &Vec<u8>,
    ) {
        let start_pos: usize =
            page_id.page_index as usize * PageCache::get_page_size();
        self.get_file()
            .seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");
        self.get_file().write(&data).expect("io error");
        self.get_file().flush().expect("io error");
    }

    pub fn clear(&self) {
        self.get_file().set_len(0).expect("io error");
    }
}

impl BTreeTable {
    pub fn set_root_pid(
        &self,
        tx: &Transaction,
        root_pid: &BTreePageID,
    ) {
        let root_pointer_rc = self.get_root_ptr_page(tx);
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
                let left_rc = Database::mut_page_cache()
                    .get_internal_page(
                        tx,
                        Permission::ReadWrite,
                        &child_pid,
                    )
                    .unwrap();

                // borrow of left_rc start here
                {
                    let mut left = left_rc.wl();
                    left.set_parent_pid(&parent_pid);
                }
                // borrow of left_rc end here
            }
            PageCategory::Leaf => {
                let child_rc = Database::mut_page_cache()
                    .get_leaf_page(
                        tx,
                        Permission::ReadWrite,
                        &child_pid,
                    )
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
    pub fn find_leaf_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        page_id: BTreePageID,
        search: &SearchFor,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        match page_id.category {
            PageCategory::Leaf => {
                // get page and return directly
                return Database::mut_page_cache()
                    .get_leaf_page(tx, perm, &page_id)
                    .unwrap();
            }
            PageCategory::Internal => {
                let page_rc = Database::mut_page_cache()
                    .get_internal_page(
                        tx,
                        Permission::ReadWrite,
                        &page_id,
                    )
                    .unwrap();
                let mut child_pid: Option<BTreePageID> = None;

                // borrow of page_rc start here
                {
                    let page = page_rc.rl();
                    let it = BTreeInternalPageIterator::new(&page);
                    let mut entry: Option<Entry> = None;
                    let mut found = false;
                    for e in it {
                        match search {
                            SearchFor::Target(cell) => {
                                if &e.get_key() >= cell {
                                    child_pid =
                                        Some(e.get_left_child());
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

                                // dont't break here, we need to find
                                // the
                                // rightmost entry
                            }
                        }
                        entry = Some(e);
                    }

                    if !found {
                        // if not found, search in right of the last
                        // entry
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
                            tx, perm, child_pid, search,
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

    /// Initialize the data file when it is necessary.
    fn file_init(
        mut file: impl DerefMut<Target = File>,
        table_inex: u32,
    ) {
        // if db file is empty, create root pointer page at first
        if file.metadata().unwrap().len() == 0 {
            // write root pointer page
            {
                let pid = BTreePageID::new(
                    PageCategory::RootPointer,
                    table_inex,
                    0,
                );

                let page = BTreeRootPointerPage::new_empty_page(&pid);
                let data = page.get_page_data();
                file.write(&data).unwrap();
            }

            // write the first leaf page
            {
                let data = BTreeBasePage::empty_page_data();
                file.write(&data).unwrap();
            }
        }
    }

    pub fn get_first_page(
        &self,
        tx: &Transaction,
        perm: Permission,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid(tx);
        return self.find_leaf_page(
            tx,
            perm,
            page_id,
            &SearchFor::LeftMost,
        );
    }

    pub fn get_last_page(
        &self,
        tx: &Transaction,
        perm: Permission,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid(tx);
        return self.find_leaf_page(
            tx,
            perm,
            page_id,
            &SearchFor::RightMost,
        );
    }

    /// Get the root page pid.
    pub fn get_root_pid(&self, tx: &Transaction) -> BTreePageID {
        let root_ptr_rc = self.get_root_ptr_page(tx);
        let mut root_pid = root_ptr_rc.rl().get_root_pid();
        root_pid.table_id = self.get_id();
        root_pid
    }

    pub fn get_root_ptr_page(
        &self,
        tx: &Transaction,
    ) -> Arc<RwLock<BTreeRootPointerPage>> {
        let root_ptr_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        Database::mut_page_cache()
            .get_root_ptr_page(
                tx,
                Permission::ReadWrite,
                &root_ptr_pid,
            )
            .unwrap()
    }

    /// The count of pages in this BTreeFile
    ///
    /// (the ROOT_POINTER page is not included)
    pub fn pages_count(&self) -> usize {
        let file_size =
            self.get_file().metadata().unwrap().len() as usize;
        debug!(
            "file size: {}, page size: {}",
            file_size,
            PageCache::get_page_size()
        );
        file_size / PageCache::get_page_size() - 1
    }

    // get the first tuple under the internal/leaf page
    pub fn get_first_tuple(
        &self,
        _pid: &BTreePageID,
    ) -> Option<Tuple> {
        todo!()
    }

    pub fn set_page_index(&self, i: u32) {
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
                let page_rc = Database::mut_page_cache()
                    .get_internal_page(tx, Permission::ReadOnly, pid)
                    .unwrap();

                // borrow of page_rc start here
                let child_pid: BTreePageID;
                {
                    let page = page_rc.rl();
                    let mut it =
                        BTreeInternalPageIterator::new(&page);
                    child_pid =
                        it.next_back().unwrap().get_right_child();
                }
                // borrow of page_rc end here
                self.get_last_tuple(tx, &child_pid)
            }
            PageCategory::Leaf => {
                let page_rc = Database::mut_page_cache()
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
    ///     - 1: print the root pointer page and the root page
    ///       (internal or leaf)
    ///     - ...
    ///     - -1: print all pages
    pub fn draw_tree(&self, max_level: i64) {
        Database::concurrent_status().clear();

        let tx = Transaction::new();
        let mut depiction = "".to_string();

        depiction.push_str(
            "\n\n----- PRINT TREE STRUCTURE START -----\n\n",
        );

        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        depiction.push_str(&format!(
            "root pointer: {}\n",
            root_pointer_pid
        ));

        let root_pid = self.get_root_pid(&tx);
        depiction.push_str(
            &self.draw_subtree(&tx, &root_pid, 0, max_level),
        );

        depiction.push_str(&format!(
            "\n\n----- PRINT TREE STRUCTURE END   -----\n\n"
        ));

        debug!("tree_structure, level {}: {}", max_level, depiction);
        tx.commit().unwrap();
    }

    fn draw_subtree(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
        level: usize,
        max_level: i64,
    ) -> String {
        match pid.category {
            PageCategory::Internal => {
                self.draw_internal_node(tx, pid, level, max_level)
            }
            PageCategory::Leaf => self.draw_leaf_node(tx, pid, level),
            _ => {
                panic!("invalid page category: {:?}", pid.category);
            }
        }
    }

    fn draw_leaf_node(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
        level: usize,
    ) -> String {
        let mut depiction = "".to_string();

        let print_sibling = false;

        let mut prefix = "│   ".repeat(level);
        let page_rc = Database::mut_page_cache()
            .get_leaf_page(tx, Permission::ReadOnly, &pid)
            .unwrap();
        let lock_state = lock_state(page_rc.clone());

        let mut it =
            BTreeLeafPageIteratorRc::new(Arc::clone(&page_rc));
        let first_tuple = it.next();

        let page = page_rc.rl();
        let mut it = BTreeLeafPageIterator::new(&page);
        let last_tuple = it.next_back();

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
        depiction.push_str(&format!(
            "{}├── first tuple: {:?}\n",
            prefix, first_tuple
        ));
        depiction.push_str(&format!(
            "{}└── last tuple:  {:?}\n",
            prefix, last_tuple
        ));

        return depiction;
    }

    fn draw_internal_node(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
        level: usize,
        max_level: i64,
    ) -> String {
        let mut depiction = "".to_string();

        let prefix = "│   ".repeat(level);
        let page_rc = Database::mut_page_cache()
            .get_internal_page(tx, Permission::ReadWrite, &pid)
            .unwrap();
        let lock_state = lock_state(page_rc.clone());

        // borrow of page_rc start here
        {
            let page = page_rc.rl();
            depiction.push_str(&format!(
                "{}├── internal: {} ({}/{} children) (lock state: {})\n",
                prefix,
                pid,
                page.children_count(),
                page.get_children_capacity(),
                lock_state,
            ));
            if max_level != -1 && level as i64 == max_level {
                return depiction;
            }
            let it = BTreeInternalPageIterator::new(&page);
            for (i, entry) in it.enumerate() {
                depiction.push_str(&self.draw_entry(
                    tx,
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
        tx: &Transaction,
        id: usize,
        entry: &Entry,
        level: usize,
        max_level: i64,
    ) -> String {
        let mut depiction = "".to_string();

        let prefix = "│   ".repeat(level);
        if id == 0 {
            depiction.push_str(&self.draw_subtree(
                tx,
                &entry.get_left_child(),
                level + 1,
                max_level,
            ));
        }
        depiction.push_str(&format!(
            "{}├── key: {:?}\n",
            prefix,
            entry.get_key()
        ));
        depiction.push_str(&self.draw_subtree(
            tx,
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
    /// require s_lock on all pages.
    ///
    /// panic on any error found.
    pub fn check_integrity(&self, check_occupancy: bool) {
        Database::concurrent_status().clear();

        let tx = Transaction::new();

        let root_ptr_page = self.get_root_ptr_page(&tx);
        let root_pid = root_ptr_page.rl().get_root_pid();
        let root_summary = self.check_sub_tree(
            &tx,
            &root_pid,
            &root_ptr_page.rl().get_pid(),
            &None,
            &None,
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

        tx.commit().unwrap();
    }

    /// panic on any error found.
    fn check_sub_tree(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
        parent_pid: &BTreePageID,
        lower_bound: &Option<Cell>,
        upper_bound: &Option<Cell>,
        check_occupancy: bool,
        depth: usize,
    ) -> SubtreeSummary {
        match pid.category {
            PageCategory::Leaf => {
                let page_rc = Database::mut_page_cache()
                    .get_leaf_page(tx, Permission::ReadOnly, &pid)
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
                let page_rc = Database::mut_page_cache()
                    .get_internal_page(
                        tx,
                        Permission::ReadWrite,
                        &pid,
                    )
                    .unwrap();
                let page = page_rc.rl();
                page.check_integrity(
                    parent_pid,
                    &lower_bound,
                    &upper_bound,
                    check_occupancy,
                    depth,
                );

                let it = BTreeInternalPageIterator::new(&page);

                let mut child_lower_bound: Option<Cell> =
                    lower_bound.clone();
                let mut summary: Option<SubtreeSummary> = None;
                let mut last_entry: Option<Entry> = None;

                for entry in it {
                    let current_summary = self.check_sub_tree(
                        tx,
                        &entry.get_left_child(),
                        pid,
                        &child_lower_bound,
                        &Some(entry.get_key()),
                        check_occupancy,
                        depth + 1,
                    );
                    match summary {
                        Some(ref mut s) => {
                            s.check_and_merge(&current_summary);
                        }
                        None => {
                            summary = Some(current_summary);
                        }
                    }

                    child_lower_bound = Some(entry.get_key());

                    last_entry = Some(entry);
                }

                let last_right_summary = self.check_sub_tree(
                    tx,
                    &last_entry.unwrap().get_right_child(),
                    pid,
                    &child_lower_bound,
                    upper_bound,
                    check_occupancy,
                    depth + 1,
                );

                match summary {
                    Some(ref mut s) => {
                        s.check_and_merge(&last_right_summary);
                        return s.clone();
                    }
                    None => {
                        return last_right_summary;
                    }
                }
            }

            // no other page types allowed inside the tree.
            _ => panic!("invalid page category"),
        }
    }
}

#[derive(Debug, Clone)]
struct SubtreeSummary {
    /// The distance towards the root.
    depth: usize,

    left_ptr: Option<BTreePageID>,
    left_most_pid: Option<BTreePageID>,
    right_ptr: Option<BTreePageID>,
    right_most_pid: Option<BTreePageID>,
}

impl SubtreeSummary {
    fn check_and_merge(&mut self, right: &SubtreeSummary) {
        assert_eq!(self.depth, right.depth);
        assert_eq!(
            self.right_ptr, right.left_most_pid,
            "depth: {}, left_ptr: {:?}, right_ptr: {:?}",
            self.depth, self.right_ptr, right.left_most_pid
        );
        assert_eq!(self.right_most_pid, right.left_ptr);

        self.right_ptr = right.right_ptr;
        self.right_most_pid = right.right_most_pid;

        // let acc = SubtreeSummary {
        //     depth: self.depth,
        //     left_ptr: self.left_ptr,
        //     left_most_pid: self.left_most_pid,
        //     right_ptr: right.right_ptr,
        //     right_most_pid: right.right_most_pid,
        // };
        // return acc;
    }
}
