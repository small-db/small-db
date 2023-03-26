use core::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    btree::buffer_pool::BufferPool, types::SmallResult, Database,
};

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

    pub fn start(&self) -> SmallResult {
        Database::mut_log_manager().log_start(self)
    }

    pub fn commit(&self) -> SmallResult {
        self.complete(true, &Database::mut_buffer_pool())
    }

    pub fn manual_commit(
        &self,
        page_cache: &BufferPool,
    ) -> SmallResult {
        self.complete(true, page_cache)
    }

    pub fn abort(&self) -> SmallResult {
        self.complete(false, &Database::mut_buffer_pool())
    }

    pub fn manual_abort(
        &self,
        page_cache: &BufferPool,
    ) -> SmallResult {
        self.complete(false, page_cache)
    }

    fn complete(
        &self,
        commit: bool,
        page_cache: &BufferPool,
    ) -> SmallResult {
        // write abort log record and rollback transaction
        if !commit {
            // does rollback too
            Database::mut_log_manager()
                .log_abort(self, page_cache)?;
        }

        // Release locks and flush pages if needed
        //
        // release locks
        page_cache.tx_complete(self, commit);

        // write commit log record
        if commit {
            Database::mut_log_manager().log_commit(self)?;
        }

        Database::concurrent_status().release_lock_by_tx(self)?;

        Ok(())
    }

    pub fn get_id(&self) -> u64 {
        self.uuid
    }
}

// This function will led to a bug:
// thread 'main' panicked at 'rwlock write lock would result in
// deadlock', /rustc/a55dd71d5fb0ec5a6a3a9e8c27b2127ba491ce52/library/
// std/src/sys/unix/ locks/pthread_rwlock.rs:111:13 impl Drop for
// Transaction {     fn drop(&mut self) {
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
