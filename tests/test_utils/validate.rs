use log::error;
use small_db::{
    btree::table::BTreeTableSearchIterator, field::IntField,
    transaction::Transaction, BTreeTable, Predicate,
};

pub fn key_present(
    tx: &Transaction,
    table: &BTreeTable,
    key: i32,
) -> bool {
    let predicate =
        Predicate::new(small_db::Op::Equals, IntField::new(key));
    let mut it = BTreeTableSearchIterator::new(tx, &table, predicate);
    it.next().is_some()
}

pub fn assert_true(predicate: bool, table: &BTreeTable) {
    if !predicate {
        error!("assertion failed, debug_info:");
        table.draw_tree(1);
        table.draw_tree(2);
        table.draw_tree(-1);
        table.check_integrity(true);
        panic!("assertion failed");
    }
}
