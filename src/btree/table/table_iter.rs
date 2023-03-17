use std::{
    mem,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use crate::{
    btree::page::{BTreeLeafPage, BTreeLeafPageIterator},
    concurrent_status::Permission,
    storage::tuple::WrappedTuple,
    transaction::Transaction,
    utils::HandyRwLock,
    BTreeTable, Database,
};

impl<'table, 'tx> BTreeTable {
    pub fn iter(
        &'table self,
        tx: &'tx Transaction,
    ) -> BTreeTableIterator2
    where
        'tx: 'table,
    {
        BTreeTableIterator2::new(tx, self)
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
        tx: &'tx Transaction,
        table: &'table BTreeTable,
    ) -> Self {
        todo!()
    }
}

// impl Iterator for BTreeTableIterator2<'_, '_> {
//     type Item = WrappedTuple;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

pub trait SleepyIterator<'this> {
    type Item;

    fn next(&'this mut self) -> Option<Self::Item>;
}

impl<'this, 'tx, 'table, 'page> SleepyIterator<'this>
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
                let sibling_rc = Database::mut_page_cache()
                    .get_leaf_page(
                        &self.tx,
                        Permission::ReadOnly,
                        &right,
                    )
                    .unwrap();
                self.page_rc = Arc::clone(&sibling_rc);
                self.page = self.page_rc.read().unwrap();
                // self.page_it = BTreeLeafPageIterator::new(&self.page);
                return self.page_it.next();
            }
            None => {
                return None;
            }
        }
    }
}
