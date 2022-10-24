use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Once,
    thread::sleep,
    time::Instant,
};

use crate::{
    btree::page::BTreePageID, error::SimpleError, transaction::Transaction,
};

pub enum Lock {
    XLock,
    SLock,
}

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
    x_lock_map: HashMap<BTreePageID, HashSet<Transaction>>,
    s_lock_map: HashMap<BTreePageID, Transaction>,
    hold_pages: HashMap<Transaction, HashSet<BTreePageID>>,
}

impl ConcurrentStatus {
    fn new() -> Self {
        Self {
            x_lock_map: HashMap::new(),
            s_lock_map: HashMap::new(),
            hold_pages: HashMap::new(),
        }
    }

    pub fn global() -> &'static mut Self {
        // Initialize it to a null value
        static mut SINGLETON: *mut ConcurrentStatus =
            0 as *mut ConcurrentStatus;
        static ONCE: Once = Once::new();

        ONCE.call_once(|| {
            // Make it
            let singleton = Self::new();

            unsafe {
                // Put it in the heap so it can outlive this call
                SINGLETON = mem::transmute(Box::new(singleton));
            }
        });

        unsafe {
            // Now we give out a copy of the data that is safe to use
            // concurrently. (*SINGLETON).clone()
            SINGLETON.as_mut().unwrap()
        }
    }

    pub fn acquire_lock(
        &mut self,
        tx: &Transaction,
        lock: Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SimpleError> {
        return Ok(());

        let start_time = Instant::now();
        while Instant::now().duration_since(start_time).as_secs() < 10 {
            match lock {
                Lock::SLock => match self.x_lock_map.get(page_id) {
                    None => {
                        return self.add_lock(tx, lock, page_id);
                    }
                    Some(v) => {
                        if v.contains(tx) {
                            return Ok(());
                        }
                    }
                },
                Lock::XLock => {
                    if self.s_lock_map.contains_key(page_id) {
                        continue;
                    }
                    if let Some(v) = self.x_lock_map.get(page_id) {
                        if v.contains(tx) {
                            return Ok(());
                        }
                        continue;
                    }
                    return self.add_lock(tx, lock, page_id);
                }
            }

            sleep(std::time::Duration::from_millis(10));
        }

        unimplemented!()
    }

    fn add_lock(
        &mut self,
        tx: &Transaction,
        lock: Lock,
        page_id: &BTreePageID,
    ) -> Result<(), SimpleError> {
        match lock {
            Lock::SLock => {
                self.s_lock_map.insert(*page_id, *tx);
            }
            Lock::XLock => {
                let mut set = HashSet::new();
                set.insert(*tx);
                self.x_lock_map.insert(*page_id, set);
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
}
