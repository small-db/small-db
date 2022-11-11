use small_db::{
    btree::table::BTreeTableSearchIterator, field::IntField,
    transaction::Transaction, BTreeTable, Predicate,
};

pub fn key_present(tx: &Transaction, table: &BTreeTable, key: i32) -> bool {
    let predicate = Predicate::new(small_db::Op::Equals, IntField::new(key));
    let mut it = BTreeTableSearchIterator::new(tx, &table, predicate);
    it.next().is_some()
}
