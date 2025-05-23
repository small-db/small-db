use std::{
    cmp,
    ops::DerefMut,
    sync::{Arc, RwLock},
    usize,
};

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
    storage::tuple::{Cell, WrappedTuple},
    transaction::{Permission, Transaction},
    types::SmallResult,
    utils::HandyRwLock,
    BTreeTable, Database, Predicate,
};

/// delete-related methods
impl BTreeTable {
    /// Delete a tuple from this BTreeFile.
    ///
    /// May cause pages to merge or redistribute entries/tuples if the
    /// pages become less than half full.
    ///
    /// TODO: remove this api
    pub fn delete_tuple(&self, tx: &Transaction, tuple: &WrappedTuple) -> SmallResult {
        let pid = tuple.get_pid();
        let leaf_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &pid).unwrap();

        // hold the leaf page
        {
            let mut leaf = leaf_rc.wl();
            leaf.mvcc_delete_tuple(&tx.get_id(), tuple.get_slot_number());
        }
        // release the leaf page

        // TODO: after implementation mvcc, only tuples which are invisible to all
        // (active) transactions should be deleted from the page.

        if !leaf_rc.rl().stable() {
            self.handle_unstable_leaf_page(tx, leaf_rc.clone())?;
        }

        let leaf_pid = leaf_rc.rl().get_pid();
        Database::mut_concurrent_status().release_latch(tx, &leaf_pid)?;

        Ok(())
    }

    /// Delete all tuples that meet the predicate from this BTreeFile.
    ///
    /// TODO: this api is too slow.
    pub fn delete_tuples(&self, tx: &Transaction, predicate: &Predicate) -> SmallResult {
        let root_pid = self.get_root_pid(tx);
        let mut page_rc =
            self.find_leaf_page(&tx, Permission::ReadWrite, root_pid, &SearchFor::LeftMost);

        // step 1: find all pages that may contian the tuples that meet the predicate

        loop {
            let slots = page_rc.rl().search(predicate);

            if slots.len() > 0 {
                for slot in &slots {
                    page_rc.wl().mvcc_delete_tuple(&tx.get_id(), slot.clone());
                }

                if !page_rc.rl().stable() {
                    self.handle_unstable_leaf_page(tx, page_rc.clone())?;
                }
            }

            let right = page_rc.rl().get_right_pid();

            // Database::mut_concurrent_status().release_lock(tx, &page_rc.rl().get_pid())?;

            if let Some(v) = right {
                page_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &v).unwrap();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Handle the case when a leaf page becomes less than half full due to
    /// deletions.
    ///
    /// If one of its siblings has extra tuples, redistribute those tuples.
    /// Otherwise merge with one of the siblings. Update pointers as needed.
    fn handle_unstable_leaf_page(
        &self,
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeLeafPage>>,
    ) -> SmallResult {
        if page_rc.rl().stable() {
            return Ok(());
        }

        let left_pid = page_rc.rl().get_left_pid();
        let right_pid = page_rc.rl().get_right_pid();

        if let Some(left_pid) = left_pid {
            let left_rc = BufferPool::get_leaf_page(tx, Permission::ReadWrite, &left_pid).unwrap();
            self.balancing_two_leaf_pages(tx, left_rc, page_rc.clone())?;
        } else if let Some(right_pid) = right_pid {
            let right_rc =
                BufferPool::get_leaf_page(tx, Permission::ReadWrite, &right_pid).unwrap();
            self.balancing_two_leaf_pages(tx, page_rc.clone(), right_rc)?;
        } else {
            let err_msg = format!(
                "page {} is unstable but has no left or right sibling",
                page_rc.rl().get_pid()
            );
            return Err(SmallError::new(&err_msg));
        };

        // release the latch on the pages:
        // - original unstable page
        // - left sibling page
        // - right sibling page
        let pid = page_rc.rl().get_pid();
        Database::mut_concurrent_status().release_latch(tx, &pid)?;
        if let Some(left_pid) = left_pid {
            Database::mut_concurrent_status().release_latch(tx, &left_pid)?;
        }
        if let Some(right_pid) = right_pid {
            Database::mut_concurrent_status().release_latch(tx, &right_pid)?;
        }

        return Ok(());
    }

    /// Handle the case when an internal page becomes less than half
    /// full due to deletions.
    ///
    /// If one of its siblings has extra entries, redistribute those
    /// entries. Otherwise merge with one of the siblings. Update
    /// pointers as needed.
    ///
    /// # Arguments
    ///
    /// - page_rc - the erratic internal page to be handled
    fn handle_unstable_internal_page(
        &self,
        tx: &Transaction,
        page_rc: Arc<RwLock<BTreeInternalPage>>,
    ) -> SmallResult {
        if page_rc.rl().get_parent_pid().category == PageCategory::RootPointer {
            return Ok(());
        }

        let left_pid = page_rc.rl().get_left_sibling_pid(tx);
        let right_pid = page_rc.rl().get_right_sibling_pid(tx);
        if let Some(left_pid) = left_pid {
            let left_rc =
                BufferPool::get_internal_page(tx, Permission::ReadWrite, &left_pid).unwrap();
            self.balancing_two_internal_pages(tx, left_rc, page_rc.clone())?;
        } else if let Some(right_pid) = right_pid {
            let right_rc =
                BufferPool::get_internal_page(tx, Permission::ReadWrite, &right_pid).unwrap();
            self.balancing_two_internal_pages(tx, page_rc.clone(), right_rc)?;
        } else {
            panic!("Cannot find the left/right sibling of the page");
        }

        Ok(())
    }

    fn set_parent_pid(&self, tx: &Transaction, child_pid: &BTreePageID, parent_pid: &BTreePageID) {
        match child_pid.category {
            PageCategory::Leaf => {
                let child_rc =
                    BufferPool::get_leaf_page(tx, Permission::ReadWrite, child_pid).unwrap();
                child_rc.wl().set_parent_pid(&parent_pid);
            }
            PageCategory::Internal => {
                let child_rc =
                    BufferPool::get_internal_page(tx, Permission::ReadOnly, child_pid).unwrap();
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
    ) -> SmallResult {
        // hold left_rc and right_rc
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

            // stage 1: pull down the edge entry from parent and
            // insert it into target page
            let edge_entry = Entry::new(
                &parent_entry.get_key(),
                &left.get_last_child_pid(),
                &right.get_first_child_pid(),
            );
            self.set_parent_pid(tx, &right.get_first_child_pid(), &left.get_pid());
            left.insert_entry(&edge_entry)?;

            // stage 2: move the entries from the one page to the
            // other
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
            self.set_empty_page(tx, &right.get_pid());
        }
        // release left_rc and right_rc

        // stage 4: update the entry in parent which points to the
        // left and right
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
    ) -> SmallResult {
        // hold the left and right page
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

            // stage 1: move the tuples from right to left
            let mut it = BTreeLeafPageIterator::new(&right);
            let mut deleted = Vec::new();
            for t in it.by_ref() {
                left.insert_tuple(&t)?;
                deleted.push(t.get_slot_number());
            }
            for slot in deleted {
                right.delete_tuple(slot);
            }

            // stage 2: update sibling pointers

            // set the right pointer of the left page to the right
            // page's right pointer
            left.set_right_pid(right.get_right_pid());

            // set the left pointer for the newer right page
            if let Some(newer_right_pid) = right.get_right_pid() {
                let newer_right_rc =
                    BufferPool::get_leaf_page(tx, Permission::ReadWrite, &newer_right_pid).unwrap();
                newer_right_rc.wl().set_left_pid(Some(left.get_pid()));
            }

            // stage 4: set the right page as empty
            self.set_empty_page(tx, &right.get_pid());
        }

        // stage 5: release the left and right page
        self.delete_parent_entry(tx, left_rc, parent_rc, entry)?;

        Ok(())
    }

    /// Method to encapsulate the process of deleting an entry
    /// (specifically the key and right child) from a parent page.
    ///
    /// If the parent becomes empty (no keys remaining), that
    /// indicates that it was the root page and should be replaced
    /// by its one remaining child.
    ///
    /// Otherwise, if it gets below minimum occupancy for non-root
    /// internal pages, it should steal from one of its siblings
    /// or merge with a sibling.
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
    ) -> SmallResult {
        // hold the parent and left page
        {
            let mut parent = parent_rc.wl();
            let mut left = left_rc.wl();

            // stage 1: delete the corresponding entry in the parent
            // page
            parent.delete_key_and_right_child(entry.get_record_id());

            // stage 2: handle the parent page according to the
            // following cases case 1: parent is empty,
            // then the left child is now the new root
            if parent.entries_count() == 0 {
                let root_ptr_page_rc = self.get_root_ptr_page(tx, Permission::ReadWrite);

                // hold the root pointer page
                {
                    let mut root_ptr_page = root_ptr_page_rc.wl();
                    left.set_parent_pid(&root_ptr_page.get_pid());
                    root_ptr_page.set_root_pid(&left.get_pid());
                }
                // release the root pointer page

                // release the latch on the root pointer page
                let root_pointer_pid = root_ptr_page_rc.rl().get_pid();
                Database::mut_concurrent_status()
                    .release_latch(tx, &root_pointer_pid)
                    .unwrap();

                // release the page for reuse
                self.set_empty_page(tx, &parent.get_pid());
                return Ok(());
            }

            // case 2: parent is stable, return directly
            if parent.stable() {
                return Ok(());
            }
        }
        // release the parent and left page

        // case 3: parent is unstable (erratic), handle it
        self.handle_unstable_internal_page(tx, parent_rc)?;
        Ok(())
    }

    /// Mark a page in this BTreeTable as empty. Find the
    /// corresponding header page (create it if needed), and mark
    /// the corresponding slot in the header page as empty.
    fn set_empty_page(&self, tx: &Transaction, pid: &BTreePageID) {
        Database::mut_buffer_pool().discard_page(pid);

        let header_pages = self.get_header_pages(tx);
        header_pages.mark_page(pid, false);
        header_pages.release_latches();
    }

    /// Balancing two internal pages according the situation:
    ///
    /// 1. Merge the two pages if the count of entries in the two
    /// pages is less than the maximum capacity of a single page.
    ///
    /// 2. Otherwise, steal entries from the sibling and copy them to
    /// the given page so that both pages are at least half full.
    ///
    /// Keys can be thought of as rotating through the parent entry,
    /// so the original key in the parent is "pulled down" to the
    /// erratic page, and the last key in the sibling page is
    /// "pushed up" to the parent.  Update parent pointers as
    /// needed.
    fn balancing_two_internal_pages(
        &self,
        tx: &Transaction,
        left_rc: Arc<RwLock<BTreeInternalPage>>,
        right_rc: Arc<RwLock<BTreeInternalPage>>,
    ) -> SmallResult {
        let parent_rc = BufferPool::get_internal_page(
            tx,
            Permission::ReadWrite,
            &left_rc.rl().get_parent_pid(),
        )
        .unwrap();
        let mut parent_entry = parent_rc
            .rl()
            .get_entry_by_children(&left_rc.rl().get_pid(), &right_rc.rl().get_pid())
            .unwrap();

        let left_children = left_rc.rl().children_count();
        let right_children = right_rc.rl().children_count();
        if left_children + right_children <= left_rc.rl().get_children_capacity() {
            // if the two pages can be merged, merge them
            return self.merge_internal_page(tx, left_rc, right_rc, parent_rc, &parent_entry);
        }

        // if there aren't any entries to move, return immediately
        let move_count =
            (left_children + right_children) / 2 - cmp::min(left_children, right_children);
        if move_count == 0 {
            return Ok(());
        }

        let mut middle_key = parent_entry.get_key();

        // hold the left and right page
        {
            let mut left = left_rc.wl();
            let mut right = right_rc.wl();

            if left_children < right_children {
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

    /// # Arguments
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
    /// # Return
    ///
    /// * The index of the moved entries in the source page.
    fn move_entries(
        &self,
        tx: &Transaction,
        src_iter: impl Iterator<Item = Entry>,
        mut dest: impl DerefMut<Target = BTreeInternalPage>,
        move_count: usize,
        middle_key: &mut Cell,
        mut edge_child_pid: BTreePageID,
        fn_get_edge_left_child: impl Fn(BTreePageID, &Entry) -> BTreePageID,
        fn_get_edge_right_child: impl Fn(BTreePageID, &Entry) -> BTreePageID,
        fn_get_moved_child: impl Fn(&Entry) -> BTreePageID,
    ) -> Result<Vec<usize>, SmallError> {
        // Remember the entries for deletion later (cause we can't
        // modify the page while iterating though it)
        let mut moved_records = Vec::new();

        for e in src_iter.take(move_count) {
            // 1. delete the entry from the src page
            moved_records.push(e.get_record_id());

            // 2. insert new entry to dest page
            let new_entry = Entry::new(
                &middle_key,
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

    /// Steal tuples from a sibling and copy them to the given page so
    /// that both pages are at least half full.  Update the
    /// parent's entry so that the key matches the key field of
    /// the first tuple in the right-hand page.
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
    ) -> SmallResult {
        let parent_rc = BufferPool::get_internal_page(
            tx,
            Permission::ReadWrite,
            &left_rc.rl().get_parent_pid(),
        )
        .unwrap();
        let mut entry = parent_rc
            .rl()
            .get_entry_by_children(&left_rc.rl().get_pid(), &right_rc.rl().get_pid())
            .unwrap();

        let left_tuples = left_rc.rl().tuples_count();
        let right_tuples = right_rc.rl().tuples_count();
        if left_tuples + right_tuples <= left_rc.rl().get_slots_count() {
            // if the two pages can be merged, merge them
            return self.merge_leaf_page(tx, left_rc, right_rc, parent_rc, &entry);
        }

        let move_count = (left_tuples + right_tuples) / 2 - cmp::min(left_tuples, right_tuples);
        if move_count == 0 {
            return self.merge_leaf_page(tx, left_rc, right_rc, parent_rc, &entry);
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
                    left.insert_tuple(&tuple)?;
                    deleted_indexes.push(tuple.get_slot_number());
                    key = tuple.get_cell(self.key_field);
                }
                for i in deleted_indexes {
                    right.delete_tuple(i);
                }
            } else {
                let iter = BTreeLeafPageIterator::new(&left);
                let mut deleted_indexes = Vec::new();
                for tuple in iter.rev().take(move_count) {
                    right.insert_tuple(&tuple)?;
                    deleted_indexes.push(tuple.get_slot_number());
                    key = tuple.get_cell(self.key_field);
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

    /// Delete all invisible tuples from the table.
    pub fn delete_invisible_tuples(&self) -> SmallResult {
        let tx = Transaction::new();

        // There is at least one active transaction since we just started one.
        let min_action = Database::concurrent_status().min_active_tx().unwrap();

        let mut page_rc: Arc<RwLock<BTreeLeafPage>> =
            self.get_first_page(&tx, Permission::ReadWrite);
        loop {
            page_rc.wl().delete_invisible_tuples(&min_action);

            self.handle_unstable_leaf_page(&tx, page_rc.clone())?;

            let right = page_rc.rl().get_right_pid();
            if let Some(right) = right {
                page_rc = BufferPool::get_leaf_page(&tx, Permission::ReadWrite, &right)?;
            } else {
                break;
            }
        }

        tx.commit().unwrap();

        Ok(())
    }
}
