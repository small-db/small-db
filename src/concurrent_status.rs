use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    thread::sleep,
    time::Instant,
};

use log::error;

use crate::{
    btree::page::BTreePageID, error::SmallError, transaction::Transaction, types::SmallResult,
    Database,
};

#[derive(Debug)]
pub enum Lock {
    XLock,
    SLock,
}

#[derive(Debug, PartialEq)]
pub enum Permission {
    ReadOnly,
    ReadWrite,
}

impl Permission {
    pub fn to_lock(&self) -> Lock {
        match self {
            Permission::ReadOnly => Lock::SLock,
            Permission::ReadWrite => Lock::XLock,
        }
    }
}

pub struct ConcurrentStatus {
    s_lock_map: HashMap<BTreePageID, HashSet<Transaction>>,
    x_lock_map: HashMap<BTreePageID, Transaction>,
    hold_pages: HashMap<Transaction, HashSet<BTreePageID>>,

    dirty_pages: HashMap<Transaction, HashSet<BTreePageID>>,
}

impl ConcurrentStatus {
    pub fn new() -> Self {
        Self {
            s_lock_map: HashMap::new(),
            x_lock_map: HashMap::new(),
            hold_pages: HashMap::new(),

            dirty_pages: HashMap::new(),
        }
    }
}

impl ConcurrentStatus {
    pub fn add_relation(&mut self, tx: &Transaction, page_id: &BTreePageID) {
        if !self.dirty_pages.contains_key(tx) {
            self.dirty_pages.insert(tx.clone(), HashSet::new());
        }

        self.dirty_pages
            .get_mut(tx)
            .unwrap()
            .insert(page_id.clone());
    }
}

impl ConcurrentStatus {
    /// Request a lock on the given page. This api is blocking.
    pub fn request_lock(
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SmallError> {
        let start_time = Instant::now();

        while Instant::now().duration_since(start_time).as_secs() < 10000 {
            if Database::mut_concurrent_status().add_lock(tx, lock, page_id)? {
                return Ok(());
            }

            sleep(std::time::Duration::from_millis(10));
        }

        error!(
            "acquire_lock timeout
            request: <tx: {}, lock: {:?}, page_id: {:?}>
            concurrent_status_map: {:?}",
            tx,
            lock,
            page_id,
            Database::concurrent_status(),
        );

        return Err(SmallError::new("acquire lock timeout"));
    }

    // Add a lock to the given page. This api is idempotent.
    //
    // Given the conditions that:
    // 1. This method could only have at most one runner at a time,
    // because it need modification actions on several maps.
    // 2. This method should not ask for exclusive permission (&mut
    // self) on the ConcurrentStatus, because we granteed that
    // multiple threads could ask for lock simultaneously (via
    // request_lock/acquire_lock).
    //
    // So, we use a unique lock to prevent this method from being
    // called by multiple threads at the same time.
    //
    // # Return
    //
    // Return a bool value to indicate whether the lock is added
    // successfully.
    fn add_lock(
        &mut self,
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<bool, SmallError> {
        // If the page hold by another transaction with X-Latch, return false (failed to
        // add lock)
        if let Some(v) = self.x_lock_map.get(page_id) {
            if v != tx {
                return Ok(false);
            }
        }

        match lock {
            Lock::SLock => {
                if !self.s_lock_map.contains_key(page_id) {
                    self.s_lock_map.insert(page_id.clone(), HashSet::new());
                }

                self.s_lock_map.get_mut(page_id).unwrap().insert(tx.clone());
            }
            Lock::XLock => {
                // If the page hold by another transaction with S-Latch, return false (failed to
                // add lock)
                if let Some(v) = self.s_lock_map.get(page_id) {
                    for tx in v {
                        if tx != tx {
                            return Ok(false);
                        }
                    }
                }

                self.x_lock_map.insert(page_id.clone(), tx.clone());
            }
        }

        if !self.hold_pages.contains_key(tx) {
            self.hold_pages.insert(tx.clone(), HashSet::new());
        }

        self.hold_pages.get_mut(tx).unwrap().insert(page_id.clone());
        return Ok(true);
    }

    fn release_lock_by_tx(&mut self, tx: &Transaction) -> SmallResult {
        if !self.hold_pages.contains_key(tx) {
            return Ok(());
        }

        let hold_pages = self.hold_pages.get(tx).unwrap().clone();
        for page_id in hold_pages {
            self.release_lock(tx, &page_id)?;
        }

        self.hold_pages.remove(tx);

        return Ok(());
    }

    fn release_lock(&mut self, tx: &Transaction, page_id: &BTreePageID) -> SmallResult {
        if let Some(v) = self.s_lock_map.get_mut(page_id) {
            v.remove(tx);
            if v.len() == 0 {
                self.s_lock_map.remove(page_id);
            }
        }

        if let Some(_) = self.x_lock_map.get_mut(page_id) {
            self.x_lock_map.remove(page_id);
        }

        return Ok(());
    }

    pub fn get_dirty_pages(&self, tx: &Transaction) -> HashSet<BTreePageID> {
        if cfg!(feature = "tree_latch") {
            return self.dirty_pages.get(tx).unwrap_or(&HashSet::new()).clone();
        } else if cfg!(feature = "page_latch") {
            return self.hold_pages.get(tx).unwrap_or(&HashSet::new()).clone();
        }

        error!("unsupported latch strategy");
        return HashSet::new();
    }

    pub fn holds_lock(&self, tx: &Transaction, page_id: &BTreePageID) -> bool {
        if let Some(v) = self.s_lock_map.get(page_id) {
            if v.contains(tx) {
                return true;
            }
        }

        if let Some(v) = self.x_lock_map.get(page_id) {
            if v == tx {
                return true;
            }
        }

        return false;
    }

    /// Remove the relation between the transaction and its related pages.
    pub fn remove_relation(&mut self, tx: &Transaction) {
        if cfg!(feature = "tree_latch") {
            self.dirty_pages.remove(tx);
        } else if cfg!(feature = "page_latch") {
            self.release_lock_by_tx(tx).unwrap();
        }
    }

    pub fn get_page_tx(&self, page_id: &BTreePageID) -> Option<Transaction> {
        if cfg!(feature = "tree_latch") {
            // For the "tree_latch" strategy, we need to check the dirty_pages map, since
            // the "x_lock_map" only contains leaf pages.
            for (tx, pages) in self.dirty_pages.iter() {
                if pages.contains(page_id) {
                    return Some(tx.clone());
                }
            }
        } else if cfg!(feature = "page_latch") {
            // For the "page_latch" strategy, the "x_lock_map" contains all pages, so we
            // can get the result directly.
            if let Some(v) = self.x_lock_map.get(page_id) {
                return Some(v.clone());
            }
        }

        return None;
    }

    pub fn clear(&mut self) {
        self.s_lock_map.clear();
        self.x_lock_map.clear();
        self.hold_pages.clear();
        self.dirty_pages.clear();
    }
}

impl fmt::Display for ConcurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut depiction = "\n".to_string();

        // s_lock_map.get_inner().rl()
        depiction.push_str("s_lock_map.get_inner().rl(): {");
        for (k, v) in self.s_lock_map.iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k.get_short_repr()));
            for tx in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", tx));
            }
            depiction.push_str("\n\t]");
        }
        depiction.push_str("\n}\n");

        // x_lock_map.get_inner().rl()
        depiction.push_str("x_lock_map.get_inner().rl(): {");
        for (k, v) in self.x_lock_map.iter() {
            depiction.push_str(&format!("\n\t{:?} -> {:?}, ", k.get_short_repr(), v));
        }
        depiction.push_str("\n}\n");

        // hold_pages
        depiction.push_str("hold_pages: {");
        for (k, v) in self.hold_pages.iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k));
            for page_id in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", page_id.get_short_repr()));
            }
            depiction.push_str("\n\t]\n");
        }

        return write!(f, "{}", depiction);
    }
}

impl fmt::Debug for ConcurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self);
    }
}
