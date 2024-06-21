use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::AtomicU32,
    thread::sleep,
    time::Instant,
};

use log::error;

use crate::{
    btree::page::BTreePageID,
    error::SmallError,
    transaction::{Transaction, TransactionID, TransactionStatus},
    types::SmallResult,
    Database,
};

static TIMEOUT: AtomicU32 = AtomicU32::new(40);

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

    // Transaction status, used for transaction isolation, the idea is from PostgreSQL.
    //
    // PostgreSQL maintains a data structure for transaction status, such that given a transaction
    // ID, it gives the transaction state (running, aborted, committed).
    pub transaction_status: HashMap<TransactionID, TransactionStatus>,
}

impl ConcurrentStatus {
    pub fn new() -> Self {
        Self {
            s_lock_map: HashMap::new(),
            x_lock_map: HashMap::new(),
            hold_pages: HashMap::new(),

            dirty_pages: HashMap::new(),

            transaction_status: HashMap::new(),
        }
    }
}

impl ConcurrentStatus {}

impl ConcurrentStatus {
    /// Set the timeout (in seconds) for acquiring a lock.
    pub fn set_timeout(timeout: u32) {
        TIMEOUT.store(timeout, std::sync::atomic::Ordering::Relaxed);
    }

    /// Request a lock on the given page. This api is blocking.
    pub(crate) fn request_lock(
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SmallError> {
        let start_time = Instant::now();

        let timeout_secs = TIMEOUT.load(std::sync::atomic::Ordering::Relaxed);

        while Instant::now().duration_since(start_time).as_secs() < timeout_secs as u64 {
            if Database::mut_concurrent_status().add_lock(tx, lock, page_id)? {
                return Ok(());
            }

            sleep(std::time::Duration::from_millis(10));
        }

        let err_msg = format!(
            "acquire lock timeout, args: {:?}, {:?}, {:?}, concurrent status: {:?}",
            tx,
            lock,
            page_id,
            Database::concurrent_status(),
        );
        let err = SmallError::new(&err_msg);
        err.show_backtrace();

        return Err(err);
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

    /// Remove the relation between the transaction and its related pages.
    pub(crate) fn remove_relation(&mut self, tx: &Transaction) {
        self.dirty_pages.remove(tx);
        self.release_locks(tx).unwrap();
    }

    fn release_locks(&mut self, tx: &Transaction) -> SmallResult {
        if !self.hold_pages.contains_key(tx) {
            return Ok(());
        }

        let hold_pages = self.hold_pages.get(tx).unwrap().clone();
        for page_id in hold_pages {
            self.release_latch(tx, &page_id)?;
        }

        self.hold_pages.remove(tx);

        return Ok(());
    }

    pub(crate) fn release_latch(&mut self, tx: &Transaction, page_id: &BTreePageID) -> SmallResult {
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

    pub(crate) fn set_dirty_page(&mut self, tx: &Transaction, page_id: &BTreePageID) {
        if !self.dirty_pages.contains_key(tx) {
            self.dirty_pages.insert(tx.clone(), HashSet::new());
        }

        self.dirty_pages
            .get_mut(tx)
            .unwrap()
            .insert(page_id.clone());
    }

    pub(crate) fn get_dirty_pages(&self, tx: &Transaction) -> HashSet<BTreePageID> {
        return self.dirty_pages.get(tx).unwrap_or(&HashSet::new()).clone();
    }

    /// Get the corresponding transaction of the dirty page, return None if the
    /// page is not a dirty page.
    pub(crate) fn dirty_page_tx(&self, page_id: &BTreePageID) -> Option<Transaction> {
        for (tx, pages) in self.dirty_pages.iter() {
            if pages.contains(page_id) {
                return Some(tx.clone());
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

        // s_lock_map
        depiction.push_str("s_lock_map.get_inner().rl(): {");
        for (k, v) in self.s_lock_map.iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k.get_short_repr()));
            for tx in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", tx));
            }
            depiction.push_str("\n\t]");
        }
        depiction.push_str("\n}\n");

        // x_lock_map
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

        // dirty_pages
        depiction.push_str("dirty_pages: {");
        for (k, v) in self.dirty_pages.iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k));
            for page_id in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", page_id.get_short_repr()));
            }
            depiction.push_str("\n\t]\n");
        }

        // transaction_status
        depiction.push_str("transaction_status: {");
        for (k, v) in self.transaction_status.iter() {
            depiction.push_str(&format!("\n\t{:?} -> {:?}, ", k, v));
        }
        depiction.push_str("\n}\n");

        return write!(f, "{}", depiction);
    }
}

impl fmt::Debug for ConcurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self);
    }
}
