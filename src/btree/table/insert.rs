use std::{
    sync::{atomic::Ordering, Arc, RwLock},
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
    concurrent_status::Permission,
    error::SmallError,
    storage::tuple::{Cell, Tuple},
    transaction::Transaction,
    types::ResultPod,
    utils::HandyRwLock,
    BTreeTable,
};

// insert-related functions
impl BTreeTable {
    /// Insert a tuple into this BTreeFile, keeping the tuples in
    /// sorted order. May cause pages to split if the page where
    /// tuple belongs is full.
    pub fn insert_tuple(&self, tx: &Transaction, tuple: &Tuple) -> Result<(), SmallError> {
        let mut new_tuple = tuple.clone();
        new_tuple.set_xmin(tx.get_id());

        if cfg!(feature = "tree_latch") {
            // Request an X-latch on the tree.
            //
            // We need the X-latch on the tree even if we don't modify the structure of the
            // tree. (e.g. the leaf page has enough space to insert the tuple). This
            // is because when we need to modify the structure of the tree (e.g.
            // split a leaf page), we need the X-latch on the tree, and their is no
            // way to upgrade the latch from S to X without gap.
            let x_latch = self.tree_latch.wl();

            let leaf_rc = self.get_available_leaf(tx, &new_tuple)?;

            // Until now, we don't have to modify the structure of the tree, just release
            // the X-latch.
            drop(x_latch);

            // Insert the tuple into the leaf page.
            leaf_rc.wl().insert_tuple(&new_tuple)?;
        } else if cfg!(feature = "page_latch") {
            let leaf_rc = self.get_available_leaf(tx, &new_tuple)?;

            // Insert the tuple into the leaf page.
            leaf_rc.wl().insert_tuple(&new_tuple)?;
        }

        return Ok(());
    }

    pub fn get_available_leaf(
        &self,
        tx: &Transaction,
        tuple: &Tuple,
    ) -> Result<Arc<RwLock<BTreeLeafPage>>, SmallError> {
        let root_pid = self.get_root_pid(tx);

        // Find and lock the left-most leaf page corresponding to the key field.
        let field = tuple.get_cell(self.key_field);
        let mut leaf_rc = self.find_leaf_page(
            tx,
            Permission::ReadWrite,
            root_pid,
            &SearchFor::Target(field),
        );

        if leaf_rc.rl().empty_slots_count() == 0 {
            // Split the leaf page if there are no more slots available.
            leaf_rc = self.split_leaf_page(tx, leaf_rc, tuple.get_cell(self.key_field))?;
        }

        Ok(leaf_rc)
    }

    /// Split a leaf page to make room for new tuples and
    /// recursively split the parent page as needed to
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
                new_sibling.insert_tuple(&tuple)?;
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
        let parent_rc = self.get_parent_with_empty_slots(tx, parent_pid, &field);

        // borrow of parent_rc start here
        // borrow of page_rc start here
        // borrow of new_sibling_rc start here
        {
            let mut parent = parent_rc.wl();
            let mut page = page_rc.wl();
            let mut new_sibling = new_sibling_rc.wl();
            let mut entry = Entry::new(&key, &page.get_pid(), &new_sibling.get_pid());

            parent.insert_entry(&mut entry)?;

            // set left pointer for the old right sibling
            if let Some(old_right_pid) = page.get_right_pid() {
                let old_right_rc =
                    BufferPool::get_leaf_page(tx, Permission::ReadWrite, &old_right_pid).unwrap();
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

    pub fn get_empty_page_index(&self, tx: &Transaction) -> u32 {
        let root_ptr_rc = self.get_root_ptr_page(tx);
        // borrow of root_ptr_rc start here
        {
            let root_ptr = root_ptr_rc.rl();
            let header_pid = root_ptr.get_header_pid();
            if let Some(header_pid) = header_pid {
                let header_rc =
                    BufferPool::get_header_page(tx, Permission::ReadOnly, &header_pid).unwrap();
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
        // create a parent page if necessary
        // this will be the new root of the tree
        match parent_id.category {
            PageCategory::RootPointer => {
                let new_parent_rc = self.get_empty_interanl_page(tx);

                // update the root pointer
                self.set_root_pid(tx, &new_parent_rc.wl().get_pid());

                new_parent_rc
            }
            PageCategory::Internal => {
                let parent_rc =
                    BufferPool::get_internal_page(tx, Permission::ReadWrite, &parent_id).unwrap();
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

            key = middle_entry.get_key();
            new_entry = Entry::new(&key, &page.get_pid(), &sibling.get_pid());
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

        if *field > key {
            sibling_rc
        } else {
            page_rc
        }
    }
}
