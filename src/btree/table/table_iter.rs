use std::sync::{Arc, RwLock};

use super::SearchFor;
use crate::{
    btree::{
        buffer_pool::BufferPool,
        page::{BTreeLeafPage, BTreeLeafPageIteratorRc, BTreePage},
    },
    storage::tuple::WrappedTuple,
    transaction::{Permission, Transaction},
    utils::HandyRwLock,
    BTreeTable, Database, Op, Predicate,
};

impl BTreeTable {
    pub fn iter(&self, tx: &Transaction) -> BTreeTableIterator {
        BTreeTableIterator::new(tx, self)
    }
}

pub struct BTreeTableIterator {
    tx: Transaction,

    page_rc: Arc<RwLock<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,

    last_page_rc: Arc<RwLock<BTreeLeafPage>>,
    last_page_it: BTreeLeafPageIteratorRc,
}

impl BTreeTableIterator {
    pub fn new(tx: &Transaction, table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page(&tx, Permission::ReadOnly);
        let last_page_rc = table.get_last_page(&tx, Permission::ReadOnly);

        Self {
            tx: tx.clone(),

            page_rc: Arc::clone(&page_rc),
            page_it: BTreeLeafPageIteratorRc::new(tx, Arc::clone(&page_rc)),

            last_page_rc: Arc::clone(&last_page_rc),
            last_page_it: BTreeLeafPageIteratorRc::new(tx, Arc::clone(&last_page_rc)),
        }
    }
}

impl Iterator for BTreeTableIterator {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let v = self.page_it.next();
            if !v.is_none() {
                return v;
            }

            // The current page is exhausted, move to the its right sibling.
            let right = self.page_rc.rl().get_right_pid();
            if let Some(right) = right {
                let sibling_rc =
                    BufferPool::get_leaf_page(&self.tx, Permission::ReadOnly, &right).unwrap();
                let page_it = BTreeLeafPageIteratorRc::new(&self.tx, Arc::clone(&sibling_rc));

                self.page_rc = Arc::clone(&sibling_rc);
                self.page_it = page_it;
                continue;
            } else {
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for BTreeTableIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let v = self.last_page_it.next_back();
        if !v.is_none() {
            return v;
        }

        let left = self.last_page_rc.rl().get_left_pid();
        match left {
            Some(left) => {
                let sibling_rc =
                    BufferPool::get_leaf_page(&self.tx, Permission::ReadOnly, &left).unwrap();
                let page_it = BTreeLeafPageIteratorRc::new(&self.tx, Arc::clone(&sibling_rc));

                self.last_page_rc = Arc::clone(&sibling_rc);
                self.last_page_it = page_it;
                return self.last_page_it.next_back();
            }
            None => {
                return None;
            }
        }
    }
}

pub struct BTreeTableSearchIterator<'t> {
    tx: &'t Transaction,

    current_page_rc: Arc<RwLock<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,
    predicate: Predicate,
    search_field: usize,
    is_key_search: bool,
}

impl<'t> BTreeTableSearchIterator<'t> {
    pub fn new(tx: &'t Transaction, table: &BTreeTable, predicate: &Predicate) -> Self {
        let start_page_rc: Arc<RwLock<BTreeLeafPage>>;
        let root_pid = table.get_root_pid(tx);

        if predicate.field_index == table.key_field {
            match predicate.op {
                Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
                    start_page_rc = table.find_leaf_page(
                        &tx,
                        Permission::ReadOnly,
                        root_pid,
                        &SearchFor::Target(predicate.cell.clone()),
                    )
                }
                Op::LessThan | Op::LessThanOrEq => {
                    start_page_rc = table.find_leaf_page(
                        &tx,
                        Permission::ReadOnly,
                        root_pid,
                        &SearchFor::LeftMost,
                    )
                }
                Op::Like => todo!(),
                Op::NotEquals => todo!(),
            }
        } else {
            start_page_rc =
                table.find_leaf_page(&tx, Permission::ReadOnly, root_pid, &SearchFor::LeftMost)
        }

        Self {
            tx,
            current_page_rc: Arc::clone(&start_page_rc),
            page_it: BTreeLeafPageIteratorRc::new(tx, Arc::clone(&start_page_rc)),
            predicate: predicate.clone(),
            search_field: predicate.field_index,
            is_key_search: predicate.field_index == table.key_field,
        }
    }

    // TODO: Short circuit on some conditions.
    fn next_inner(&mut self) -> Option<WrappedTuple> {
        loop {
            let tuple = self.page_it.next();

            match tuple {
                Some(t) => match self.predicate.op {
                    Op::Equals => {
                        let field = t.get_cell(self.search_field);
                        if field == self.predicate.cell {
                            return Some(t);
                        } else if self.is_key_search && field > self.predicate.cell {
                            return None;
                        }
                    }
                    Op::GreaterThan => {
                        let field = t.get_cell(self.search_field);
                        if field > self.predicate.cell {
                            return Some(t);
                        }
                    }
                    Op::GreaterThanOrEq => {
                        let field = t.get_cell(self.search_field);
                        if field >= self.predicate.cell {
                            return Some(t);
                        }
                    }
                    Op::LessThan => {
                        let field = t.get_cell(self.search_field);
                        if field < self.predicate.cell {
                            return Some(t);
                        } else if self.is_key_search && field >= self.predicate.cell {
                            return None;
                        }
                    }
                    Op::LessThanOrEq => {
                        let field = t.get_cell(self.search_field);
                        if field <= self.predicate.cell {
                            return Some(t);
                        } else if self.is_key_search && field > self.predicate.cell {
                            return None;
                        }
                    }
                    Op::Like => todo!(),
                    Op::NotEquals => todo!(),
                },
                None => {
                    let right = self.current_page_rc.rl().get_right_pid();

                    // don't need the previous page anymore, release the latch on it
                    let pid = self.current_page_rc.rl().get_pid();
                    Database::mut_concurrent_status()
                        .release_latch(self.tx, &pid)
                        .unwrap();

                    // init iterator on next page and continue search
                    if let Some(right_pid) = right {
                        let rc =
                            BufferPool::get_leaf_page(self.tx, Permission::ReadOnly, &right_pid)
                                .unwrap();
                        self.current_page_rc = Arc::clone(&rc);
                        self.page_it = BTreeLeafPageIteratorRc::new(self.tx, Arc::clone(&rc));
                        continue;
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

impl Iterator for BTreeTableSearchIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(t) = self.next_inner() {
            return Some(t);
        } else {
            // release the latch on the last page
            let pid = self.current_page_rc.rl().get_pid();
            Database::mut_concurrent_status()
                .release_latch(self.tx, &pid)
                .unwrap();

            return None;
        }
    }
}
