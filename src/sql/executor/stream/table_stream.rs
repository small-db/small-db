use std::sync::{Arc, RwLock};

use crate::{
    btree::table::BTreeTableIterator, error::SmallError, transaction::Transaction, BTreeTable,
};

use super::{Batch, Stream};

use crate::utils::HandyRwLock;

pub struct TableStream {
    iter: BTreeTableIterator,
}

impl TableStream {
    pub fn new(tx: &Transaction, table: Arc<RwLock<BTreeTable>>) -> Self {
        let iter = BTreeTableIterator::new(tx, &table.rl());
        Self { iter }
    }
}

impl Stream for TableStream {
    fn next_batch(&mut self) -> Result<Option<Batch>, SmallError> {
        let mut tuples = Vec::new();

        for _ in 0..100 {
            match self.iter.next() {
                Some(tuple) => tuples.push(tuple.get_tuple().clone()),
                None => break,
            }
        }

        Ok(Some(Batch::new(tuples)))
    }
}
