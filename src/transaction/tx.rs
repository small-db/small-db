use core::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use log::debug;

use crate::{btree::buffer_pool::BufferPool, types::SmallResult, Database};

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
        let instance = Self { uuid: id };
        instance.start().unwrap();
        instance
    }

    fn start(&self) -> SmallResult {
        Database::mut_log_manager().log_start(self)
    }

    pub fn commit(&self) -> SmallResult {
        let mut log_manager = &mut Database::mut_log_manager();
        let buffer_pool = &mut Database::mut_buffer_pool();

        // step 1: flush all related pages to disk (with "UPDATE" log record)
        //
        // (this is a disk operation, hence should be put before the "COMMIT" record is written)
        buffer_pool.flush_pages(self, &mut log_manager);

        // step 2: write "COMMIT" log record
        log_manager.log_commit(self)?;

        // step 3: release latch on dirty pages
        //
        // (this is a memory operation, hence can be put after the "COMMIT" record is written)
        Database::mut_concurrent_status().remove_relation(self);

        Ok(())
    }

    pub fn abort(&self) -> SmallResult {
        let buffer_pool = &mut Database::mut_buffer_pool();

        // step 1: write abort log record and rollback transaction
        //
        // (this operation include necessary disk operations)
        Database::mut_log_manager().log_abort(self, buffer_pool)?;

        // step 2: discard all dirty pages
        //
        // (this is a memory operation, hence can be put after the "ABORT" record is written)
        for pid in Database::concurrent_status().get_dirty_pages(self) {
            buffer_pool.discard_page(&pid);
        }

        // step 3: remove relation between transaction and dirty pages
        //
        // (this is a memory operation, hence can be put after the "COMMIT" record is written)
        //
        // (this operation should be put after the step 2, since the step 2 accesses these
        // dirty pages)
        Database::mut_concurrent_status().remove_relation(self);

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
