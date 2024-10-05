use std::{
    sync::{Arc, RwLock, RwLockWriteGuard},
    usize,
};

use env_logger::builder;

use super::SearchFor;
use crate::{
    btree::{
        buffer_pool::BufferPool,
        page::{
            BTreeInternalPage, BTreeInternalPageIterator, BTreeLeafPage, BTreeLeafPageIterator,
            BTreePage, BTreePageID, Entry, PageCategory,
        },
    },
    error::SmallError,
    storage::tuple::{Cell, Tuple},
    transaction::{Permission, Transaction},
    types::{ResultPod, SmallResult},
    utils::HandyRwLock,
    BTreeTable, Database,
};

struct Latches {
    pages: Vec<Arc<RwLock<BTreeInternalPage>>>,
}

impl Latches {
    fn new() -> Self {
        Self { pages: vec![] }
    }

    fn push(&mut self, page: Arc<RwLock<BTreeInternalPage>>) {
        self.pages.push(page);
    }

    // fn last(&'a self) -> RwLockWriteGuard<'a, dyn BTreePage> {
    //     // todo!()
    //     // let v = self.pages.last().unwrap();
    //     // v.write().unwrap()

    //     return self.pages.last().unwrap().write().unwrap();
    // }

    fn last_internal(&self) -> RwLockWriteGuard<'_, BTreeInternalPage> {
        let v = self.pages.last().unwrap();
        v.write().unwrap()
    }

    fn last_category(&self) -> PageCategory {
        todo!()
    }
}

impl BTreeTable {
    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    pub fn crab_insert_tuple(&self, tx: &Transaction, tuple: &Tuple) -> Result<(), SmallError> {
        let root_ptr_rc = self.get_root_ptr_page(tx, Permission::ReadWrite);
        let root_pid = root_ptr_rc.rl().get_root_pid();
        return self.crab_insert_to_subtree(tx, &root_pid, None, tuple);
    }

    /// Insert a tuple into the subtree whose root is the "pid", may cause the root to split.
    pub fn crab_insert_to_subtree(
        &self,
        tx: &Transaction,
        pid: &BTreePageID,
        parent: Option<RwLockWriteGuard<'_, BTreeInternalPage>>,
        tuple: &Tuple,
    ) -> SmallResult {
        match pid.category {
            PageCategory::Internal => {
                let page_rc = BufferPool::get_internal_page(tx, Permission::ReadWrite, &pid)?;
                let mut page = page_rc.write().unwrap();
                self.crab_insert_to_internal(tx, page, parent, tuple)?;
                return Ok(());
            }
            PageCategory::Leaf => {
                let page_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &pid)?;
                let mut page = page_rc.write().unwrap();
                self.crab_insert_to_leaf(tx, page, parent, tuple)?;
                return Ok(());
            }
            _ => {
                return Err(SmallError::new("Invalid page category"));
            }
        }
    }

    /// Insert a tuple into the leaf page, may cause the page to split.
    fn crab_insert_to_leaf(
        &self,
        tx: &Transaction,
        mut page: RwLockWriteGuard<'_, BTreeLeafPage>,
        mut parent: Option<RwLockWriteGuard<'_, BTreeInternalPage>>,
        tuple: &Tuple,
    ) -> SmallResult {
        if page.empty_slots_count() > 0 {
            return page.insert_tuple(tuple);
        }

        let key = tuple.get_cell(self.key_field);

        let new_sibling_rc = self.get_empty_leaf_page(tx);
        let mut new_sibling = new_sibling_rc.wl();

        let tuple_count = page.tuples_count();
        let move_tuple_count = tuple_count / 2;

        let mut it = BTreeLeafPageIterator::new(&page);
        let mut delete_indexes: Vec<usize> = Vec::new();
        for tuple in it.by_ref().rev().take(move_tuple_count) {
            delete_indexes.push(tuple.get_slot_number());
            new_sibling.insert_tuple(&tuple)?;
        }

        for i in delete_indexes {
            page.delete_tuple(i);
        }

        let mut it = BTreeLeafPageIterator::new(&page);
        let split_point = it.next_back().unwrap().get_cell(self.key_field);

        if parent.is_none() {
            let parent_rc = self.get_empty_interanl_page(tx);

            // set the new parent as the root of the tree
            self.set_root_pid(tx, &parent_rc.wl().get_pid());

            let mut parent = parent_rc.wl();
            let mut entry = Entry::new(&key, &page.get_pid(), &new_sibling.get_pid());
            parent.insert_entry(&mut entry)?;
        }

        if key > split_point {
            return new_sibling.insert_tuple(tuple);
        } else {
            return page.insert_tuple(tuple);
        }
    }

    /// Insert a tuple into the subtree whose root is the "page", may cause the page to split.
    fn crab_insert_to_internal(
        &self,
        tx: &Transaction,
        mut page: RwLockWriteGuard<'_, BTreeInternalPage>,
        mut parent: Option<RwLockWriteGuard<'_, BTreeInternalPage>>,
        tuple: &Tuple,
    ) -> SmallResult {
        let key = tuple.get_cell(self.key_field);

        let mut child_pid_opt: Option<BTreePageID> = None;

        let it = BTreeInternalPageIterator::new(&page);
        let mut entry: Option<Entry> = None;
        let mut found = false;
        for e in it {
            if e.get_key() >= key {
                child_pid_opt = Some(e.get_left_child());
                found = true;
                break;
            }

            entry = Some(e);
        }

        if !found {
            // if not found, search in right of the last
            // entry
            match entry {
                Some(e) => {
                    child_pid_opt = Some(e.get_right_child());
                }
                None => todo!(),
            }
        }

        match child_pid_opt {
            Some(child_pid) => match child_pid.category {
                PageCategory::Internal => {
                    let child_rc =
                        BufferPool::get_internal_page(tx, Permission::ReadWrite, &child_pid)?;
                    let mut child = child_rc.write().unwrap();
                    self.crab_insert_to_internal(tx, child, Some(page), tuple)?;
                    return Ok(());
                }
                PageCategory::Leaf => {
                    let child_rc =
                        BufferPool::get_leaf_page(tx, Permission::ReadWrite, &child_pid)?;
                    let child = child_rc.write().unwrap();
                    self.crab_insert_to_leaf(tx, child, Some(page), tuple)?;
                    return Ok(());
                }
                _ => {
                    return Err(SmallError::new("Invalid page category"));
                }
            },
            None => {
                todo!()
            }
        }
    }
}
