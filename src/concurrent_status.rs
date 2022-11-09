use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    thread::sleep,
    time::Instant,
};

use log::debug;

use crate::{
    btree::page::BTreePageID, error::SimpleError, transaction::Transaction,
    types::SimpleResult,
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

/// reference:
/// - https://sourcegraph.com/github.com/XiaochenCui/simple-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/simpledb/ConcurrentStatus.java?L12:14&subtree=true
pub struct ConcurrentStatus {
    s_lock_map: HashMap<BTreePageID, HashSet<Transaction>>,
    x_lock_map: HashMap<BTreePageID, Transaction>,
    hold_pages: HashMap<Transaction, HashSet<BTreePageID>>,
}

impl ConcurrentStatus {
    pub fn new() -> Self {
        Self {
            s_lock_map: HashMap::new(),
            x_lock_map: HashMap::new(),
            hold_pages: HashMap::new(),
        }
    }

    pub fn acquire_lock(
        &mut self,
        tx: &Transaction,
        lock: Lock,
        page_id: &BTreePageID,
    ) -> SimpleResult {
        // return Ok(());

        let start_time = Instant::now();
        while Instant::now().duration_since(start_time).as_secs() < 3 {
            match lock {
                Lock::SLock => match self.x_lock_map.get(page_id) {
                    Some(x_lock_tx) => {
                        if x_lock_tx == tx {
                            return Ok(());
                        }
                    }
                    None => match self.s_lock_map.get(page_id) {
                        None => {
                            return self.add_lock(tx, lock, page_id);
                        }
                        Some(v) => {
                            if v.contains(tx) {
                                return Ok(());
                            } else {
                                return self.add_lock(tx, lock, page_id);
                            }
                        }
                    },
                },
                Lock::XLock => match self.x_lock_map.get(page_id) {
                    None => match self.s_lock_map.get(page_id) {
                        None => {
                            return self.add_lock(tx, lock, page_id);
                        }
                        Some(v) => {
                            if v.contains(tx) {
                                return self.add_lock(tx, lock, page_id);
                            }
                        }
                    },
                    Some(v) => {
                        if v == tx {
                            return Ok(());
                        }
                    }
                },
            }

            // debug!("try to acquire lock, tx: {}, lock: {:?}, page_id: {:?},
            // concurrent_status: {:?}", tx, lock, page_id, self);

            // panic!("not implemented");

            sleep(std::time::Duration::from_millis(10));
        }

        debug!(
            "acquire_lock timeout, tx: {}, lock: {:?}, page_id: {:?}, concurrent_status_map: {:?}",
            tx, lock, page_id, self,
        );

        panic!("acquire_lock timeout");

        return Err(SimpleError::new("acquire lock timeout"));
    }

    fn add_lock(
        &mut self,
        tx: &Transaction,
        lock: Lock,
        page_id: &BTreePageID,
    ) -> SimpleResult {
        match lock {
            Lock::SLock => {
                let mut set = HashSet::new();
                set.insert(*tx);
                self.s_lock_map.insert(*page_id, set);
            }
            Lock::XLock => {
                self.x_lock_map.insert(*page_id, *tx);
            }
        }

        self.hold_pages
            .entry(*tx)
            .and_modify(|v| {
                v.insert(*page_id);
            })
            .or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(*page_id);
                set
            });

        return Ok(());
    }

    pub fn release_lock_by_tx(&mut self, tx: &Transaction) -> SimpleResult {
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

    fn release_lock(
        &mut self,
        tx: &Transaction,
        page_id: &BTreePageID,
    ) -> SimpleResult {
        if let Some(v) = self.s_lock_map.get_mut(page_id) {
            debug!("release_lock_shared, tx: {}, page_id: {:?}", tx, page_id);
            v.remove(tx);
            if v.len() == 0 {
                self.s_lock_map.remove(page_id);
            }
        }

        if let Some(_) = self.x_lock_map.get_mut(page_id) {
            debug!(
                "release_lock_exclusive, tx: {}, page_id: {:?}",
                tx, page_id
            );
            self.x_lock_map.remove(page_id);
        }

        return Ok(());
    }

    pub fn clear(&mut self) {
        self.s_lock_map.clear();
        self.x_lock_map.clear();
        self.hold_pages.clear();
    }
}

impl fmt::Display for ConcurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut depiction = "\n".to_string();

        // s_lock_map
        depiction.push_str("s_lock_map: {");
        for (k, v) in &self.s_lock_map {
            depiction.push_str(&format!("\n\t{:?} -> [", k.get_short_repr()));
            for tx in v {
                depiction.push_str(&format!("\n\t\t{:?}, ", tx));
            }
            depiction.push_str("\n\t]");
        }
        depiction.push_str("\n}\n");

        // x_lock_map
        depiction.push_str("x_lock_map: {");
        for (k, v) in &self.x_lock_map {
            depiction.push_str(&format!(
                "\n\t{:?} -> {:?}, ",
                k.get_short_repr(),
                v
            ));
        }
        depiction.push_str("\n}\n");

        // hold_pages
        depiction.push_str("hold_pages: {");
        for (k, v) in &self.hold_pages {
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
