use std::sync::atomic::{AtomicU64, Ordering};

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

    pub fn commit(&self) {
        // TODO
    }

    pub fn abort(&self) {
        // TODO
    }
}
