use crate::{
    storage::tuple::WrappedTuple, transaction::Transaction,
    BTreeTable,
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

pub struct BTreeTableIterator2<'tx, 'table> {
    tx: &'tx Transaction,
    table: &'table BTreeTable,
}

impl<'tx, 'table> BTreeTableIterator2<'tx, 'table> {
    pub fn new(
        tx: &'tx Transaction,
        table: &'table BTreeTable,
    ) -> Self {
        todo!()
    }
}

impl Iterator for BTreeTableIterator2<'_, '_> {
    type Item = WrappedTuple;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for BTreeTableIterator2<'_, '_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
