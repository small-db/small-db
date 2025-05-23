use core::fmt;
use std::{
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Seek, SeekFrom, Write},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard, RwLock,
    },
    time::SystemTime,
    usize,
};

use log::{self, debug};

use super::BTreeTableIterator;
use crate::{
    btree::{
        buffer_pool::BufferPool,
        page::{
            BTreeHeaderPage, BTreeInternalPage, BTreeInternalPageIterator, BTreeLeafPage,
            BTreeLeafPageIterator, BTreeLeafPageIteratorRc, BTreePage, BTreePageID, BTreePageInit,
            BTreeRootPointerPage, Entry, HeaderPages, PageCategory, TableIndex, FIRST_LEAF_PID,
        },
    },
    error::{get_caller, SmallError},
    storage::{
        table_schema::TableSchema,
        tuple::{Cell, Tuple, WrappedTuple},
    },
    transaction::{Permission, Transaction},
    types::SmallResult,
    utils::{lock_state, HandyRwLock},
    Database,
};

pub enum SearchFor {
    Target(Cell),
    LeftMost,
    RightMost,
}

/// # B+ Tree
///
/// This is a traditional B+ tree implementation. It only stores the data in
/// the leaf pages.
///
/// ## Latching Strategy
///
/// A tree latch protects all non-leaf pages in the tree. Eacho page of the
/// tree also has a latch of its own.
///
/// A B-tree operation normally first acquires an S-latch on the tree. It
/// searches down the tree and releases the tree latch when it has the
/// leaf page latch.
pub struct BTreeTable {
    pub name: String,

    // the field which index is keyed on
    pub key_field: usize,

    // the tuple descriptor of tuples in the file
    pub schema: TableSchema,

    file: Mutex<File>,

    table_id: u32,

    /// the page index of the last page in the file
    ///
    /// The page index start from 0 and increase monotonically by 1,
    /// the page index of "root pointer" page is always 0.
    pub(crate) page_index: AtomicU32,
}

#[derive(Copy, Clone)]
pub enum WriteScene {
    Random,
    Sequential,
}

impl fmt::Display for BTreeTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<BTreeFile, file: {:?}, id: {}>", "empty", self.table_id)
    }
}

// init functions
impl BTreeTable {
    pub fn new(table_name: &str, table_id: Option<u32>, schema: &TableSchema) -> Self {
        let db_path = Database::global().get_path();
        let filename = table_name.to_owned() + ".table";
        let table_path = db_path.join(filename);

        let f = Mutex::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(table_path)
                .unwrap(),
        );

        let table_id = match table_id {
            Some(id) => id,
            None => {
                let mut hasher = DefaultHasher::new();
                table_name.hash(&mut hasher);

                let unix_time = SystemTime::now();
                unix_time.hash(&mut hasher);

                hasher.finish() as u32
            }
        };

        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let instance = Self {
            name: table_name.to_string(),

            schema: schema.clone(),
            file: f,
            table_id,

            // start from 1 (the root page)
            //
            // TODO: init it according to actual condition
            page_index: AtomicU32::new(1),

            key_field: schema.get_key_pos(),
        };

        instance.file_init();
        instance
    }
}

// normal read-only functions
impl BTreeTable {
    pub fn get_id(&self) -> TableIndex {
        self.table_id
    }

    pub fn get_schema(&self) -> TableSchema {
        self.schema.clone()
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

// api which interacting with disk directly
impl BTreeTable {
    pub(crate) fn get_empty_leaf_page(&self, tx: &Transaction) -> Arc<RwLock<BTreeLeafPage>> {
        // create the new page
        let pid = BTreePageID::new(
            PageCategory::Leaf,
            self.table_id,
            self.get_empty_page_index(tx),
        );
        let page = BTreeLeafPage::new_empty_page(&pid, &self.schema);

        self.write_page_to_disk(&pid, &page.get_page_data(&self.schema));

        let page_rc = Arc::new(RwLock::new(page));
        // insert to buffer pool because it's a dirty page at this
        // time
        Database::mut_buffer_pool()
            .leaf_buffer
            .insert(pid, page_rc.clone());
        page_rc
    }

    pub(crate) fn get_empty_interanl_page(
        &self,
        tx: &Transaction,
    ) -> Arc<RwLock<BTreeInternalPage>> {
        // create the new page
        let page_id = BTreePageID::new(
            PageCategory::Internal,
            self.table_id,
            self.get_empty_page_index(tx),
        );
        let page = BTreeInternalPage::new_empty_page(&page_id, &self.schema);

        self.write_page_to_disk(&page_id, &page.get_page_data(&self.schema));

        let page_rc = Arc::new(RwLock::new(page));
        // insert to buffer pool because it's a dirty page at this
        // time
        Database::mut_buffer_pool()
            .internal_buffer
            .insert(page_id, page_rc.clone());
        page_rc
    }

    pub(crate) fn write_page_to_disk(&self, page_id: &BTreePageID, data: &Vec<u8>) {
        let start_pos: usize = page_id.page_index as usize * BufferPool::get_page_size();
        self.get_file()
            .seek(SeekFrom::Start(start_pos as u64))
            .expect("io error");
        self.get_file().write(&data).expect("io error");
        self.get_file().flush().expect("io error");
    }
}

impl BTreeTable {
    pub fn set_root_pid(&self, tx: &Transaction, root_pid: &BTreePageID) {
        let root_pointer_rc = self.get_root_ptr_page(tx, Permission::ReadWrite);
        root_pointer_rc.wl().set_root_pid(root_pid);

        // release the latch on the root pointer page
        let root_pointer_pid = root_pointer_rc.rl().get_pid();
        Database::mut_concurrent_status()
            .release_latch(tx, &root_pointer_pid)
            .unwrap();
    }

    /// Finds and locks the leaf page in the B+ tree based on the search
    /// condition.
    ///
    /// All internal pages along the path to the target page are locked
    /// with read-only permission. In "tree latch" mode, the lock will
    /// be released before the method returns. In "page latch" mode, the
    /// lock will be held until the transaction is committed.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to be used.
    /// * `perm` - The permission with which to lock the leaf page.
    /// * `pid` - The starting page ID for the search.
    /// * `search` - The key field to search for.
    ///
    /// # Returns
    ///
    /// The left-most leaf page that matches the search condition. If the search
    /// is for a specific value, the scope of this page will cover that value.
    pub(crate) fn find_leaf_page(
        &self,
        tx: &Transaction,
        perm: Permission,
        pid: BTreePageID,
        search: &SearchFor,
    ) -> Arc<RwLock<BTreeLeafPage>> {
        match pid.category {
            PageCategory::Leaf => {
                // return directly
                let v = BufferPool::get_leaf_page(tx, perm, &pid);
                if v.is_err() {
                    log::error!("error: {:?}", v);
                }
                return v.unwrap();
            }
            PageCategory::Internal => {
                let page_rc =
                    BufferPool::get_internal_page(tx, Permission::ReadOnly, &pid).unwrap();
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

                Database::mut_concurrent_status()
                    .release_latch(tx, &pid)
                    .unwrap();

                // search child page recursively
                match child_pid {
                    Some(child_pid) => {
                        return self.find_leaf_page(tx, perm, child_pid, search);
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

    /// Initialize the data file when the file is empty. Specifically, it
    /// writes the root pointer page and the an empty leaf page to the file.
    fn file_init(&self) {
        let mut file = self.get_file();
        let table_index = self.get_id();

        // if db file is empty, write essential pages:
        //  - root pointer page
        //  - the first header page
        //  - the first leaf page
        if file.metadata().unwrap().len() == 0 {
            // write root pointer page
            {
                let pid = BTreePageID::get_root_ptr_pid(table_index);
                let page = BTreeRootPointerPage::new_empty_page(&pid, &self.schema);
                let data = page.get_page_data(&self.schema);
                file.write(&data).unwrap();
            }

            // write the first header page
            {
                let pid = BTreePageID::get_header_pid(table_index);
                let mut page = BTreeHeaderPage::new_empty_page(&pid, &self.schema);
                for i in 0..3 {
                    page.mark_slot_status(i as usize, true);
                }
                let data = page.get_page_data(&self.schema);
                file.write(&data).unwrap();
            }

            // write the first leaf page
            {
                let pid = BTreePageID::new(PageCategory::Leaf, table_index, FIRST_LEAF_PID);
                let page = BTreeLeafPage::new_empty_page(&pid, &self.schema);
                let data = page.get_page_data(&self.schema);
                file.write(&data).unwrap();
            }
        }
    }

    pub fn get_first_page(&self, tx: &Transaction, perm: Permission) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid(tx);
        return self.find_leaf_page(tx, perm, page_id, &SearchFor::LeftMost);
    }

    pub fn get_last_page(&self, tx: &Transaction, perm: Permission) -> Arc<RwLock<BTreeLeafPage>> {
        let page_id = self.get_root_pid(tx);
        return self.find_leaf_page(tx, perm, page_id, &SearchFor::RightMost);
    }

    /// Get the root page pid.
    pub fn get_root_pid(&self, tx: &Transaction) -> BTreePageID {
        let root_ptr_rc = self.get_root_ptr_page(tx, Permission::ReadOnly);
        let mut root_pid = root_ptr_rc.rl().get_root_pid();
        root_pid.table_id = self.get_id();

        // release the latch on the root pointer page
        let root_pointer_pid = root_ptr_rc.rl().get_pid();
        Database::mut_concurrent_status()
            .release_latch(tx, &root_pointer_pid)
            .unwrap();

        root_pid
    }

    pub(crate) fn get_root_ptr_page(
        &self,
        tx: &Transaction,
        perm: Permission,
    ) -> Arc<RwLock<BTreeRootPointerPage>> {
        let root_ptr_pid = BTreePageID::get_root_ptr_pid(self.table_id);
        BufferPool::get_root_ptr_page(tx, perm, &root_ptr_pid).unwrap()
    }

    pub(crate) fn get_header_pages(&self, tx: &Transaction) -> HeaderPages {
        HeaderPages::new(self, tx)
    }

    /// The count of pages in this BTreeFile
    ///
    /// (the ROOT_POINTER page is not included)
    pub fn pages_count(&self) -> usize {
        let file_size = self.get_file().metadata().unwrap().len() as usize;
        file_size / BufferPool::get_page_size() - 1
    }

    // get the first tuple under the internal/leaf page
    pub fn get_first_tuple(&self, _pid: &BTreePageID) -> Option<Tuple> {
        todo!()
    }

    pub fn set_page_index(&self, i: u32) {
        self.page_index.store(i, Ordering::Relaxed);
    }

    // get the last tuple under the internal/leaf page
    pub fn get_last_tuple(&self, tx: &Transaction, pid: &BTreePageID) -> Option<WrappedTuple> {
        match pid.category {
            PageCategory::RootPointer => todo!(),
            PageCategory::Internal => {
                let page_rc = BufferPool::get_internal_page(tx, Permission::ReadOnly, pid).unwrap();

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
                let page_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, pid).unwrap();

                let page = page_rc.rl();
                let mut it = BTreeLeafPageIterator::new(&page);
                it.next_back()
            }
            PageCategory::Header => todo!(),
        }
    }

    pub(crate) fn get_empty_page_index(&self, tx: &Transaction) -> u32 {
        let header_pages = self.get_header_pages(tx);
        let empty_page_index = header_pages.get_empty_page_index();
        header_pages.release_latches();
        empty_page_index as u32
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
    pub fn draw_tree(&self, max_level: i64) {
        Database::mut_concurrent_status().clear();

        let tx = Transaction::new();

        let mut depiction = "".to_string();

        depiction.push_str("\n\n----- PRINT TREE STRUCTURE START -----\n\n");

        // get root pointer page
        let root_pointer_pid = BTreePageID {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: self.table_id,
        };
        depiction.push_str(&format!("root pointer: {}\n", root_pointer_pid));

        let root_pid = self.get_root_pid(&tx);
        depiction.push_str(&self.draw_subtree(&tx, &root_pid, 0, max_level));

        depiction.push_str(&format!("\n\n----- PRINT TREE STRUCTURE END   -----\n\n"));

        debug!(
            "[{}] tree_structure, level {}: {}",
            get_caller(),
            max_level,
            depiction
        );
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
            PageCategory::Internal => self.draw_internal_node(tx, pid, level, max_level),
            PageCategory::Leaf => self.draw_leaf_node(tx, pid, level),
            _ => {
                panic!("invalid page category: {:?}", pid.category);
            }
        }
    }

    fn draw_leaf_node(&self, tx: &Transaction, pid: &BTreePageID, level: usize) -> String {
        let mut depiction = "".to_string();

        let print_sibling = false;
        let print_all_tuples = false;

        let mut prefix = "│   ".repeat(level);
        let page_rc = BufferPool::get_leaf_page(tx, Permission::ReadOnly, &pid).unwrap();
        let lock_state = lock_state(page_rc.clone());

        let mut it = BTreeLeafPageIteratorRc::new(tx, Arc::clone(&page_rc));
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
                page.get_slots_count(),
                lock_state,
            ));
        }

        prefix = "│   ".repeat(level + 1);
        if print_all_tuples {
            let it = BTreeLeafPageIterator::new(&page);
            for tuple in it {
                depiction.push_str(&format!("{}├── tuple: {:?}\n", prefix, tuple));
            }
        } else {
            depiction.push_str(&format!("{}├── first tuple: {:?}\n", prefix, first_tuple));
            depiction.push_str(&format!("{}└── last tuple:  {:?}\n", prefix, last_tuple));
        }

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
        let page_rc = BufferPool::get_internal_page(tx, Permission::ReadOnly, &pid).unwrap();
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
                depiction.push_str(&self.draw_entry(tx, i, &entry, level + 1, max_level));
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
        depiction.push_str(&format!("{}├── key: {:?}\n", prefix, entry.get_key()));
        depiction.push_str(&self.draw_subtree(tx, &entry.get_right_child(), level + 1, max_level));

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
    ///
    /// TODO: remove argument `check_occupancy` and always check
    /// occupancy.
    pub fn check_integrity(&self) {
        if let Err(e) = self.check_integrity_inner(true) {
            e.show_backtrace();
            self.draw_tree(-1);
            panic!();
        }
    }

    fn check_integrity_inner(&self, check_occupancy: bool) -> SmallResult {
        Database::mut_concurrent_status().clear();

        let tx = Transaction::new();

        let root_ptr_page = self.get_root_ptr_page(&tx, Permission::ReadOnly);
        let root_pid = root_ptr_page.rl().get_root_pid();
        let root_summary = self.check_sub_tree(
            &tx,
            &root_pid,
            &root_ptr_page.rl().get_pid(),
            &None,
            &None,
            check_occupancy,
            0,
        )?;
        if root_summary.left_ptr.is_some() {
            // the "root_summary" stands for the whole tree, so the left pointer should be
            // none.
            let err_msg = format!("root left pointer is not none: {:?}", root_summary.left_ptr);
            return Err(SmallError::new(&err_msg));
        }
        if root_summary.right_ptr.is_some() {
            // the "root_summary" stands for the whole tree, so the right pointer should be
            // none.
            let err_msg = format!(
                "root right pointer is not none: {:?}",
                root_summary.right_ptr
            );
            return Err(SmallError::new(&err_msg));
        }

        tx.commit().unwrap();

        Ok(())
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
    ) -> Result<SubtreeSummary, SmallError> {
        match pid.category {
            PageCategory::Leaf => {
                let page_rc = BufferPool::get_leaf_page(tx, Permission::ReadOnly, &pid).unwrap();
                let page = page_rc.rl();
                page.check_integrity(parent_pid, lower_bound, upper_bound, check_occupancy, depth)?;

                let summary = SubtreeSummary {
                    left_ptr: page.get_left_pid(),
                    right_ptr: page.get_right_pid(),

                    left_most_pid: Some(page.get_pid()),
                    right_most_pid: Some(page.get_pid()),

                    depth,
                };
                return Ok(summary);
            }

            PageCategory::Internal => {
                let page_rc =
                    BufferPool::get_internal_page(tx, Permission::ReadOnly, &pid).unwrap();
                let page = page_rc.rl();
                page.check_integrity(
                    parent_pid,
                    &lower_bound,
                    &upper_bound,
                    check_occupancy,
                    depth,
                )?;

                let it = BTreeInternalPageIterator::new(&page);

                let mut child_lower_bound: Option<Cell> = lower_bound.clone();
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
                    )?;
                    match summary {
                        Some(ref mut s) => {
                            s.check_and_merge(&current_summary)?;
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
                )?;

                match summary {
                    Some(ref mut s) => {
                        s.check_and_merge(&last_right_summary)?;
                        return Ok(s.clone());
                    }
                    None => {
                        return Ok(last_right_summary);
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
    fn check_and_merge(&mut self, right: &SubtreeSummary) -> SmallResult {
        if self.depth != right.depth {
            // the depth of the two subtrees should be the same
            let err_msg = format!("depth mismatch: {} != {}", self.depth, right.depth,);
            return Err(SmallError::new(&err_msg));
        }

        assert_eq!(self.depth, right.depth);

        if self.right_ptr != right.left_most_pid {
            // the right pointer of the left subtree should be the leftmost
            // page of the right subtree
            let err_msg = format!(
                "right pointer mismatch: {:?} != {:?}",
                self.right_ptr, right.left_most_pid,
            );
            return Err(SmallError::new(&err_msg));
        }

        if self.right_most_pid != right.left_ptr {
            // the rightmost page of the left subtree should be the left
            // pointer of the right subtree
            let err_msg = format!(
                "rightmost page mismatch: {:?} != {:?}",
                self.right_most_pid, right.left_ptr,
            );
            return Err(SmallError::new(&err_msg));
        }

        // merge the two summaries
        self.right_ptr = right.right_ptr;
        self.right_most_pid = right.right_most_pid;

        Ok(())
    }
}
