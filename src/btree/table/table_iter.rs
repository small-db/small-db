use std::sync::{Arc, RwLock, RwLockReadGuard};

use super::SearchFor;
use crate::{
    btree::page::{
        BTreeLeafPage, BTreeLeafPageIterator, BTreeLeafPageIteratorRc,
    },
    concurrent_status::Permission,
    storage::tuple::WrappedTuple,
    transaction::Transaction,
    utils::HandyRwLock,
    BTreeTable, Database, Op, Predicate,
};

impl<'table, 'tx> BTreeTable {
    pub fn iter(
        &'table self,
        tx: &'tx Transaction,
    ) -> BTreeTableIterator
    where
        'tx: 'table,
    {
        BTreeTableIterator::new(tx, self)
    }
}

pub struct BTreeTableIterator<'t> {
    tx: &'t Transaction,

    page_rc: Arc<RwLock<BTreeLeafPage>>,
    page_it: BTreeLeafPageIteratorRc,

    last_page_rc: Arc<RwLock<BTreeLeafPage>>,
    last_page_it: BTreeLeafPageIteratorRc,
}

impl<'t> BTreeTableIterator<'t> {
    pub fn new(tx: &'t Transaction, table: &BTreeTable) -> Self {
        let page_rc = table.get_first_page(tx, Permission::ReadOnly);
        let last_page_rc =
            table.get_last_page(tx, Permission::ReadOnly);

        Self {
            tx,

            page_rc: Arc::clone(&page_rc),
            page_it: BTreeLeafPageIteratorRc::new(Arc::clone(
                &page_rc,
            )),

            last_page_rc: Arc::clone(&last_page_rc),
            last_page_it: BTreeLeafPageIteratorRc::new(Arc::clone(
                &last_page_rc,
            )),
        }
    }
}

impl Iterator for BTreeTableIterator<'_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.page_it.next();
        if !v.is_none() {
            return v;
        }

        let right = self.page_rc.rl().get_right_pid();
        match right {
            Some(right) => {
                let sibling_rc = Database::mut_buffer_pool()
                    .get_leaf_page(
                        &self.tx,
                        Permission::ReadOnly,
                        &right,
                    )
                    .unwrap();
                let page_it = BTreeLeafPageIteratorRc::new(
                    Arc::clone(&sibling_rc),
                );

                self.page_rc = Arc::clone(&sibling_rc);
                self.page_it = page_it;
                return self.page_it.next();
            }
            None => {
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for BTreeTableIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let v = self.last_page_it.next_back();
        if !v.is_none() {
            return v;
        }

        let left = self.last_page_rc.rl().get_left_pid();
        match left {
            Some(left) => {
                let sibling_rc = Database::mut_buffer_pool()
                    .get_leaf_page(
                        self.tx,
                        Permission::ReadOnly,
                        &left,
                    )
                    .unwrap();
                let page_it = BTreeLeafPageIteratorRc::new(
                    Arc::clone(&sibling_rc),
                );

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
    key_field: usize,
}

impl<'t> BTreeTableSearchIterator<'t> {
    pub fn new(
        tx: &'t Transaction,
        table: &BTreeTable,
        index_predicate: &Predicate,
    ) -> Self {
        let start_rc: Arc<RwLock<BTreeLeafPage>>;
        let root_pid = table.get_root_pid(tx);

        match index_predicate.op {
            Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
                start_rc = table.find_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    root_pid,
                    &SearchFor::Target(index_predicate.field.clone()),
                )
            }
            Op::LessThan | Op::LessThanOrEq => {
                start_rc = table.find_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    root_pid,
                    &SearchFor::LeftMost,
                )
            }
            Op::Like => todo!(),
            Op::NotEquals => todo!(),
        }

        Self {
            tx,
            current_page_rc: Arc::clone(&start_rc),
            page_it: BTreeLeafPageIteratorRc::new(Arc::clone(
                &start_rc,
            )),
            predicate: index_predicate.clone(),
            key_field: table.key_field,
        }
    }
}

impl Iterator for BTreeTableSearchIterator<'_> {
    type Item = WrappedTuple;

    // TODO: Short circuit on some conditions.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let tuple = self.page_it.next();
            match tuple {
                Some(t) => match self.predicate.op {
                    Op::Equals => {
                        let field = t.get_cell(self.key_field);
                        if field == self.predicate.field {
                            return Some(t);
                        } else if field > self.predicate.field {
                            return None;
                        }
                    }
                    Op::GreaterThan => {
                        let field = t.get_cell(self.key_field);
                        if field > self.predicate.field {
                            return Some(t);
                        }
                    }
                    Op::GreaterThanOrEq => {
                        let field = t.get_cell(self.key_field);
                        if field >= self.predicate.field {
                            return Some(t);
                        }
                    }
                    Op::LessThan => {
                        let field = t.get_cell(self.key_field);
                        if field < self.predicate.field {
                            return Some(t);
                        } else if field >= self.predicate.field {
                            return None;
                        }
                    }
                    Op::LessThanOrEq => {
                        let field = t.get_cell(self.key_field);
                        if field <= self.predicate.field {
                            return Some(t);
                        } else if field > self.predicate.field {
                            return None;
                        }
                    }
                    Op::Like => todo!(),
                    Op::NotEquals => todo!(),
                },
                None => {
                    // init iterator on next page and continue search
                    let right =
                        (*self.current_page_rc).rl().get_right_pid();
                    match right {
                        Some(pid) => {
                            let rc = Database::mut_buffer_pool()
                                .get_leaf_page(
                                    self.tx,
                                    Permission::ReadOnly,
                                    &pid,
                                )
                                .unwrap();
                            self.current_page_rc = Arc::clone(&rc);
                            self.page_it =
                                BTreeLeafPageIteratorRc::new(
                                    Arc::clone(&rc),
                                );
                            continue;
                        }
                        None => {
                            return None;
                        }
                    }
                }
            }
        }
    }
}

pub struct BTreeTableIterator2<'tx, 'page> {
    tx: &'tx Transaction,

    page_rc: Arc<RwLock<BTreeLeafPage>>,
    page: RwLockReadGuard<'page, BTreeLeafPage>,
    page_it: BTreeLeafPageIterator<'page>,
}

impl<'tx, 'table, 'page> BTreeTableIterator2<'tx, 'page> {
    pub fn new(
        _tx: &'tx Transaction,
        _table: &'table BTreeTable,
    ) -> Self {
        todo!()
    }
}

pub trait NestedIterator<'this> {
    type Item;

    fn next(&'this mut self) -> Option<Self::Item>;
}

impl<'this, 'tx, 'table, 'page> NestedIterator<'this>
    for BTreeTableIterator2<'tx, 'page>
where
    'this: 'page,
{
    type Item = WrappedTuple;

    fn next(&'this mut self) -> Option<Self::Item> {
        let v = self.page_it.next();
        if !v.is_none() {
            return v;
        }

        let right = self.page_it.page.get_right_pid();
        match right {
            Some(right) => {
                let sibling_rc = Database::mut_buffer_pool()
                    .get_leaf_page(
                        &self.tx,
                        Permission::ReadOnly,
                        &right,
                    )
                    .unwrap();
                self.page_rc = Arc::clone(&sibling_rc);
                self.page = self.page_rc.read().unwrap();
                self.page_it = BTreeLeafPageIterator::new(&self.page);
                return self.page_it.next();
            }
            None => {
                return None;
            }
        }
    }
}
