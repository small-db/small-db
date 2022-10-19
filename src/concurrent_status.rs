use std::collections::{HashMap, HashSet};

use crate::{
    btree::page::BTreePageID, error::MyError, transaction::Transaction,
};

/// reference:
/// - https://sourcegraph.com/github.com/XiaochenCui/simple-db-hw@87607789b677d6afee00a223eacb4f441bd4ae87/-/blob/src/java/simpledb/ConcurrentStatus.java?L12:14&subtree=true
struct ConcurrentStatus {
    x_lock_map: HashMap<BTreePageID, HashSet<Transaction>>,
    s_lock_map: HashMap<BTreePageID, Transaction>,
    hold_pages: HashMap<Transaction, HashSet<BTreePageID>>,
}

pub enum LockType {
    XLock,
    SLock,
}

impl ConcurrentStatus {
    pub fn acquire_lock(
        _tx: &Transaction,
        _page_id: BTreePageID,
        _lock_type: LockType,
    ) -> Result<(), MyError> {
        unimplemented!()
    }
}
