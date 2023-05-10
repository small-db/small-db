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

#[derive(Debug)]
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

pub enum AcquireResult {
    Acquired,
    Granted,
}

/// reference:
/// - https://sourcegraph.com/github.com/XiaochenCui/small-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/smalldb/ConcurrentStatus.java?L12:14&subtree=true
pub struct ConcurrentStatus {
    s_lock_map: ConcurrentHashMap<BTreePageID, HashSet<Transaction>>,
    x_lock_map: ConcurrentHashMap<BTreePageID, Transaction>,
    pub hold_pages:
        ConcurrentHashMap<Transaction, HashSet<BTreePageID>>,
    modification_lock: Arc<Mutex<()>>,
}

impl ConcurrentStatus {
    pub fn new() -> Self {
        Self {
            s_lock_map: ConcurrentHashMap::new(),
            x_lock_map: ConcurrentHashMap::new(),
            hold_pages: ConcurrentHashMap::new(),
            modification_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn request_lock(
        &self,
        tx: &Transaction,
        lock: &Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SmallError> {
        let start_time = Instant::now();
        while Instant::now().duration_since(start_time).as_secs()
            < 100
        {
            if Database::concurrent_status()
                .add_lock(tx, lock, page_id)?
            {
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

        if !self.x_lock_map.exact_or_empty(page_id, tx) {
            return Ok(false);
        }

        match lock {
            Lock::SLock => {
                self.s_lock_map.alter_value(
                    page_id,
                    |s_lock_set| {
                        s_lock_set.insert(tx.clone());
                        Ok(())
                    },
                )?;
            }
            Lock::XLock => {
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

        // debug!(
        //     "lock_acquired, tx: {}, lock: {:?}, page_id: {:?}",
        //     tx, lock, page_id
        // );
        return Ok(true);
    }

    pub fn release_lock_by_tx(
        &self,
        tx: &Transaction,
    ) -> SmallResult {
        if !self.hold_pages.get_inner().rl().contains_key(tx) {
            return Ok(());
        }

        let hold_pages =
            self.hold_pages.get_inner().rl().get(tx).unwrap().clone();
        for page_id in hold_pages {
            self.release_lock(tx, &page_id)?;
        }

        self.hold_pages.remove(tx);

        return Ok(());
    }

    fn release_lock(
        &self,
        tx: &Transaction,
        page_id: &BTreePageID,
    ) -> SmallResult {
        let mut s_lock_map = self.s_lock_map.get_inner_wl();
        if let Some(v) = s_lock_map.get_mut(page_id) {
            // debug!(
            //     "release_lock_shared, tx: {}, page_id: {:?}",
            //     tx, page_id
            // );
            v.remove(tx);
            if v.len() == 0 {
                s_lock_map.remove(page_id);
            }
        }

        let mut x_lock_map = self.x_lock_map.get_inner_wl();
        if let Some(_) = x_lock_map.get_mut(page_id) {
            // debug!(
            //     "release_lock_exclusive, tx: {}, page_id: {:?}",
            //     tx, page_id
            // );
            x_lock_map.remove(page_id);
        }

        return Ok(());
    }

    pub fn holds_lock(
        &self,
        tx: &Transaction,
        page_id: &BTreePageID,
    ) -> bool {
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

    pub fn get_page_tx(
        &self,
        page_id: &BTreePageID,
    ) -> Option<Transaction> {
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
            depiction.push_str(&format!(
                "\n\t{:?} -> [",
                k.get_short_repr()
            ));
            for tx in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", tx));
            }
            depiction.push_str("\n\t]");
        }
        depiction.push_str("\n}\n");

        // x_lock_map.get_inner().rl()
        depiction.push_str("x_lock_map.get_inner().rl(): {");
        for (k, v) in self.x_lock_map.get_inner().rl().iter() {
            depiction.push_str(&format!(
                "\n\t{:?} -> {:?}, ",
                k.get_short_repr(),
                v
            ));
        }
        depiction.push_str("\n}\n");

        // hold_pages
        depiction.push_str("hold_pages: {");
        for (k, v) in self.hold_pages.get_inner().rl().iter() {
            depiction.push_str(&format!("\n\t{:?} -> [", k));
            for page_id in v {
                depiction.push_str(&format!(
                    "\n\t\t{:?}, ",
                    page_id.get_short_repr()
                ));
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
