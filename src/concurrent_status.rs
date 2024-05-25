use core::fmt;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    thread::sleep,
    time::Instant,
};

use log::error;

use crate::{
    btree::page::BTreePageID,
    error::SmallError,
    transaction::Transaction,
    types::{ConcurrentHashMap, SmallResult},
    utils::HandyRwLock,
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
    s_lock_map: ConcurrentHashMap<BTreePageID, HashSet<Transaction>>,
    x_lock_map: ConcurrentHashMap<BTreePageID, Transaction>,
    pub hold_pages: ConcurrentHashMap<Transaction, HashSet<BTreePageID>>,

    // TODO: what is this lock for? Can we just remove it?
    modification_lock: Arc<Mutex<()>>,

    dirty_pages: ConcurrentHashMap<Transaction, HashSet<BTreePageID>>,
}

impl ConcurrentStatus {
    pub fn new() -> Self {
        Self {
            s_lock_map: ConcurrentHashMap::new(),
            x_lock_map: ConcurrentHashMap::new(),
            hold_pages: ConcurrentHashMap::new(),
            modification_lock: Arc::new(Mutex::new(())),

            dirty_pages: ConcurrentHashMap::new(),
        }
    }

    pub fn add_relation(&self, tx: &Transaction, page_id: &BTreePageID) {
        self.dirty_pages
            .alter_value(tx, |dirty_pages_set| {
                dirty_pages_set.insert(*page_id);
                Ok(())
            })
            .unwrap();
    }

    pub fn remove_relation(&self, tx: &Transaction) {
        self.dirty_pages.remove(tx);
    }

    pub fn get_dirty_pages(&self, tx: &Transaction) -> HashSet<BTreePageID> {
        return self
            .dirty_pages
            .get_inner_rl()
            .get(tx)
            .unwrap_or(&HashSet::new())
            .clone();
    }

    // Get related transaction of a page (throught dirty_pages)
    pub fn get_page_tx2(&self, page_id: &BTreePageID) -> Option<Transaction> {
        for (tx, pages) in self.dirty_pages.get_inner_rl().iter() {
            if pages.contains(page_id) {
                return Some(tx.clone());
            }
        }

        return None;
    }

    /// Request a lock on the given page. This api is blocking.
    pub fn request_lock(
        &self,
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SmallError> {
        let start_time = Instant::now();

        while Instant::now().duration_since(start_time).as_secs() < 30 {
            if Database::concurrent_status().add_lock(tx, lock, page_id)? {
                return Ok(());
            }

            sleep(std::time::Duration::from_millis(10));
        }

        error!(
            "acquire_lock timeout
            request: <tx: {}, lock: {:?}, page_id: {:?}>
            concurrent_status_map: {:?}",
            tx, lock, page_id, self,
        );

        panic!("acquire_lock timeout");

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
        &self,
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<bool, SmallError> {
        let _guard = self.modification_lock.lock().unwrap();

        // If the page hold by another transaction with X-Latch, return false (failed to add lock)
        if let Some(v) = self.x_lock_map.get_inner_rl().get(page_id) {
            if v != tx {
                return Ok(false);
            }
        }

        match lock {
            Lock::SLock => {
                self.s_lock_map.alter_value(page_id, |s_lock_set| {
                    s_lock_set.insert(tx.clone());
                    Ok(())
                })?;
            }
            Lock::XLock => {
                // If the page hold by another transaction with S-Latch, return false (failed to add lock)
                if let Some(v) = self.s_lock_map.get_inner_rl().get(page_id) {
                    for tx in v {
                        if tx != tx {
                            return Ok(false);
                        }
                    }
                }

                self.x_lock_map
                    .get_inner()
                    .wl()
                    .insert(page_id.clone(), tx.clone());
            }
        }

        self.hold_pages.alter_value(tx, |hold_pages_set| {
            hold_pages_set.insert(*page_id);
            Ok(())
        })?;

        return Ok((true));
    }

    pub fn release_lock_by_tx(&self, tx: &Transaction) -> SmallResult {
        if !self.hold_pages.get_inner().rl().contains_key(tx) {
            return Ok(());
        }

        let hold_pages = self.hold_pages.get_inner().rl().get(tx).unwrap().clone();
        for page_id in hold_pages {
            self.release_lock(tx, &page_id)?;
        }

        self.hold_pages.remove(tx);

        return Ok(());
    }

    fn release_lock(&self, tx: &Transaction, page_id: &BTreePageID) -> SmallResult {
        let mut s_lock_map = self.s_lock_map.get_inner_wl();
        if let Some(v) = s_lock_map.get_mut(page_id) {
            v.remove(tx);
            if v.len() == 0 {
                s_lock_map.remove(page_id);
            }
        }

        let mut x_lock_map = self.x_lock_map.get_inner_wl();
        if let Some(_) = x_lock_map.get_mut(page_id) {
            x_lock_map.remove(page_id);
        }

        return Ok(());
    }

    pub fn holds_lock(&self, tx: &Transaction, page_id: &BTreePageID) -> bool {
        let s_lock_map = self.s_lock_map.get_inner_rl();
        let x_lock_map = self.x_lock_map.get_inner_rl();

        if let Some(v) = s_lock_map.get(page_id) {
            if v.contains(tx) {
                return true;
            }
        }

        if let Some(v) = x_lock_map.get(page_id) {
            if v == tx {
                return true;
            }
        }

        return false;
    }

    pub fn get_page_tx(&self, page_id: &BTreePageID) -> Option<Transaction> {
        let x_lock_map = self.x_lock_map.get_inner_rl();
        if let Some(v) = x_lock_map.get(page_id) {
            return Some(v.clone());
        }

        return None;
    }

    pub fn clear(&self) {
        self.s_lock_map.get_inner().wl().clear();
        self.x_lock_map.get_inner().wl().clear();
        self.hold_pages.clear();
    }
}

impl fmt::Display for ConcurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut depiction = "\n".to_string();

        // s_lock_map.get_inner().rl()
        depiction.push_str("s_lock_map.get_inner().rl(): {");
        for (k, v) in self.s_lock_map.get_inner().rl().iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k.get_short_repr()));
            for tx in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", tx));
            }
            depiction.push_str("\n\t]");
        }
        depiction.push_str("\n}\n");

        // x_lock_map.get_inner().rl()
        depiction.push_str("x_lock_map.get_inner().rl(): {");
        for (k, v) in self.x_lock_map.get_inner().rl().iter() {
            depiction.push_str(&format!("\n\t{:?} -> {:?}, ", k.get_short_repr(), v));
        }
        depiction.push_str("\n}\n");

        // hold_pages
        depiction.push_str("hold_pages: {");
        for (k, v) in self.hold_pages.get_inner().rl().iter() {
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
