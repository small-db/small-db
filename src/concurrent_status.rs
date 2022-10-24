use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Once,
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
        // unimplemented!()
        Ok(())
    }
}
