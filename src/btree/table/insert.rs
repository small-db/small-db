use std::{
    sync::{Arc, RwLock, RwLockWriteGuard},
    usize,
};

use crate::{
    btree::{
        buffer_pool::BufferPool,
        page::{
            BTreeInternalPage, BTreeInternalPageIterator, BTreeLeafPage, BTreeLeafPageIterator,
            BTreePage, BTreePageID, Entry, PageCategory,
        },
    },
    error::SmallError,
    storage::tuple::Tuple,
    transaction::{Permission, Transaction},
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable,
};

enum Action {
    /// Current page doesn't need to split/merge to perform the given action,
    /// release all latches of its ancestors.
    Release,

    /// Current page needs to split to perform the insert action, insert the
    /// split entry to its parent page.
    InsertEntry(Entry),
}

impl BTreeTable {
    /// Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
    pub fn insert_tuple(&self, tx: &Transaction, tuple: &Tuple) -> Result<(), SmallError> {
        let root_pointer_rc = self.get_root_ptr_page(tx, Permission::ReadWrite);
        let mut root_pointer = root_pointer_rc.wl();

        let root_pid = root_pointer.get_root_pid();

        let root_ptr_callback = |action: &Action| match action {
            Action::Release => {
                drop(root_pointer);
            }
            Action::InsertEntry(entry) => {
                let new_root_rc = self.get_empty_interanl_page(tx);
                let mut new_root = new_root_rc.wl();

                new_root.insert_entry(&entry).unwrap();
                root_pointer.set_root_pid(&new_root.get_pid());
            }
        };

        match root_pid.category {
            PageCategory::Internal => {
                let page_rc = BufferPool::get_internal_page(tx, Permission::ReadWrite, &root_pid)?;
                let page = page_rc.write().unwrap();
                self.insert_to_internal(tx, page, root_ptr_callback, tuple)?;
                return Ok(());
            }
            PageCategory::Leaf => {
                let page_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &root_pid)?;
                let page = page_rc.write().unwrap();
                self.insert_to_leaf(tx, page, root_ptr_callback, tuple)?;
                return Ok(());
            }
            _ => {
                return Err(SmallError::new("Invalid page category"));
            }
        }
    }

    /// Insert a tuple into the leaf page, may cause the page to split.
    fn insert_to_leaf(
        &self,
        tx: &Transaction,
        mut page: RwLockWriteGuard<'_, BTreeLeafPage>,
        parent_callback: impl FnOnce(&Action),
        tuple: &Tuple,
    ) -> SmallResult {
        if page.empty_slots_count() > 0 {
            parent_callback(&Action::Release);
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

        // set sibling id
        new_sibling.set_right_pid(page.get_right_pid());
        new_sibling.set_left_pid(Some(page.get_pid()));
        page.set_right_pid(Some(new_sibling.get_pid()));

        let mut it = BTreeLeafPageIterator::new(&page);
        let split_point = it.next_back().unwrap().get_cell(self.key_field);

        let entry = Entry::new(&key, &page.get_pid(), &new_sibling.get_pid());
        parent_callback(&Action::InsertEntry(entry));

        if key > split_point {
            return new_sibling.insert_tuple(tuple);
        } else {
            return page.insert_tuple(tuple);
        }
    }

    /// Insert a tuple into the subtree whose root is the "page", may cause the
    /// page to split.
    fn insert_to_internal(
        &self,
        tx: &Transaction,
        mut page: RwLockWriteGuard<'_, BTreeInternalPage>,
        parent_callback: impl FnOnce(&Action),
        tuple: &Tuple,
    ) -> SmallResult {
        if page.empty_slots_count() > 0 {
            parent_callback(&Action::Release);
            return self.insert_to_internal_safe(tx, page, tuple);
        } else {
            let sibling_rc = self.get_empty_interanl_page(tx);
            let mut sibling = sibling_rc.wl();

            let enties_count = page.entries_count();
            let move_entries_count = enties_count / 2;

            let mut delete_indexes: Vec<usize> = Vec::new();
            let mut it = BTreeInternalPageIterator::new(&page);
            for e in it.by_ref().rev().take(move_entries_count) {
                delete_indexes.push(e.get_record_id());
                sibling.insert_entry(&e).unwrap();

                // set parent id for the right child
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
            Self::set_parent(tx, &middle_entry.get_right_child(), &sibling.get_pid());

            let split_point = middle_entry.get_key();
            let new_entry = Entry::new(&split_point, &page.get_pid(), &sibling.get_pid());

            let parent_pid = page.get_parent_pid();
            page.set_parent_pid(&parent_pid);
            sibling.set_parent_pid(&parent_pid);

            parent_callback(&Action::InsertEntry(new_entry));

            let key = tuple.get_cell(self.key_field);
            if key > split_point {
                return self.insert_to_internal_safe(tx, sibling, tuple);
            } else {
                return self.insert_to_internal_safe(tx, page, tuple);
            }
        }
    }

    fn insert_to_internal_safe(
        &self,
        tx: &Transaction,
        mut page: RwLockWriteGuard<'_, BTreeInternalPage>,
        tuple: &Tuple,
    ) -> SmallResult {
        if page.empty_slots_count() == 0 {
            return Err(SmallError::new(
                "no empty slots, this api should be called only when there is empty slots",
            ));
        }

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

        let internal_callback = |action: &Action| match action {
            Action::Release => {
                // already release the latch of ancestors before, no need to do
                // that here.
                drop(page);
            }
            Action::InsertEntry(entry) => {
                page.insert_entry(&entry).unwrap();
            }
        };

        match child_pid_opt {
            Some(child_pid) => match child_pid.category {
                PageCategory::Internal => {
                    let child_rc =
                        BufferPool::get_internal_page(tx, Permission::ReadWrite, &child_pid)?;
                    let child = child_rc.write().unwrap();
                    self.insert_to_internal(tx, child, internal_callback, tuple)?;
                    return Ok(());
                }
                PageCategory::Leaf => {
                    let child_rc =
                        BufferPool::get_leaf_page(tx, Permission::ReadWrite, &child_pid)?;
                    let child = child_rc.write().unwrap();
                    self.insert_to_leaf(tx, child, internal_callback, tuple)?;
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
