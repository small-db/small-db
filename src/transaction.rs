use core::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{types::SmallResult, Unique};

static TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
pub struct Transaction {
    // increase monotonically by 1
    uuid: u64,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            uuid: TRANSACTION_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn commit(&self) -> SmallResult {
        Unique::concurrent_status().release_lock_by_tx(self)
    }

    pub fn abort(&self) {
        // TODO
    }
}

// This function will led to a bug:
// thread 'main' panicked at 'rwlock write lock would result in deadlock', /rustc/a55dd71d5fb0ec5a6a3a9e8c27b2127ba491ce52/library/std/src/sys/unix/locks/pthread_rwlock.rs:111:13
// impl Drop for Transaction {
//     fn drop(&mut self) {
//         self.commit().unwrap();
//     }
// }

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "tx_{}", self.uuid)
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self);
    }
}
