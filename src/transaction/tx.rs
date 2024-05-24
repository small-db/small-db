use core::fmt;
use std::{
    collections::HashSet,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    btree::{buffer_pool::BufferPool, page::BTreePageID},
    types::SmallResult,
    Database,
};

static TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Eq, PartialEq, Clone)]
pub struct Transaction {
    // increase monotonically by 1
    uuid: TransactionID,
}

pub type TransactionID = u64;

impl Transaction {
    pub fn new() -> Self {
        let id = TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);
        Self::new_specific_id(id)
    }

    pub fn new_specific_id(id: u64) -> Self {
        Self { uuid: id }
    }

    pub fn start(&self) -> SmallResult {
        Database::mut_log_manager().log_start(self)
    }

    pub fn commit(&self) -> SmallResult {
        let mut log_manager = &mut Database::mut_log_manager();
        let buffer_pool = &mut Database::mut_buffer_pool();
        buffer_pool.flush_pages(self, &mut log_manager);

        // write "COMMIT" log record
        log_manager.log_commit(self)?;

        // Database::concurrent_status().release_lock_by_tx(self)?;
        Database::concurrent_status().remove_relation(self);

        Ok(())
    }

    pub fn abort(&self) -> SmallResult {
        self.complete(false, &mut Database::mut_buffer_pool())
    }

    fn complete(&self, commit: bool, buffer_pool: &mut BufferPool) -> SmallResult {
        // write abort log record and rollback transaction
        if !commit {
            // does rollback too
            Database::mut_log_manager().log_abort(self, buffer_pool)?;
        }

        // Release locks and flush pages if needed
        //
        // release locks
        buffer_pool.tx_complete(self, commit);

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

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

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
