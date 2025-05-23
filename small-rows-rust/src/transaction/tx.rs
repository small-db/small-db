use core::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::{types::SmallResult, Database};

#[derive(Clone, PartialEq, Debug)]
pub enum TransactionStatus {
    Active,
    Aborted,
    Committed,
}

pub type TransactionID = u32;

pub(crate) const TRANSACTION_ID_BYTES: usize = 4;

static TRANSACTION_ID: AtomicU32 = AtomicU32::new(1);

#[derive(PartialEq, Eq, Clone)]
pub struct Transaction {
    // increase monotonically by 1
    id: TransactionID,
}

impl Transaction {
    pub fn new() -> Self {
        let id = TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);
        let instance = Self { id };
        instance.start().unwrap();

        Database::mut_concurrent_status().set_transaction_status(&id, &TransactionStatus::Active);

        instance
    }

    fn start(&self) -> SmallResult {
        Database::mut_log_manager().log_start(self)
    }

    pub fn commit(&self) -> SmallResult {
        Database::mut_concurrent_status()
            .set_transaction_status(&self.id, &TransactionStatus::Committed);
        return Ok(());

        // step 1: flush all related pages to disk (with "UPDATE" log record)
        //
        // (this is a disk operation, hence should be put before the "COMMIT" record is
        // written)
        Database::mut_buffer_pool().flush_pages(self, &mut Database::mut_log_manager());

        // step 2: write "COMMIT" log record
        Database::mut_log_manager().log_commit(self)?;

        if cfg!(feature = "aries_no_force") {
            Database::mut_buffer_pool().write_pages(self);
        }

        // step 3: release latch on dirty pages
        //
        // (this is a memory operation, hence can be put after the "COMMIT" record is
        // written)
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
        // (this is a memory operation, hence can be put after the "ABORT" record is
        // written)
        for pid in Database::concurrent_status().get_dirty_pages(self) {
            buffer_pool.discard_page(&pid);
        }

        // step 3: remove relation between transaction and dirty pages
        //
        // (this is a memory operation, hence can be put after the "COMMIT" record is
        // written)
        //
        // (this operation should be put after the step 2, since the step 2 accesses
        // these dirty pages)
        Database::mut_concurrent_status().remove_relation(self);

        Database::mut_concurrent_status()
            .set_transaction_status(&self.id, &TransactionStatus::Aborted);

        Ok(())
    }

    pub fn get_id(&self) -> TransactionID {
        self.id
    }
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "tx_{}", self.id)
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self);
    }
}
