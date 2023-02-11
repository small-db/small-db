use log::error;
use small_db::{
    btree::table::BTreeTableSearchIterator, field::IntField,
    transaction::Transaction, BTreeTable, Op, Predicate, Unique,
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

pub fn look_for(
    table: &BTreeTable,
    tx: &Transaction,
    key: i32,
) -> usize {
    let predicate = Predicate::new(Op::Equals, IntField::new(key));
    let it = BTreeTableSearchIterator::new(&tx, &table, predicate);
    return it.count();
}

pub fn assert_true(predicate: bool, table: &BTreeTable) {
    if !predicate {
        error!("--- assertion failed, debug_info start ---");
        Unique::log_file().show_log_contents();
        panic!("assertion failed");
        table.draw_tree(-1);
        table.check_integrity(true);
        error!("--- assertion failed, debug_info end ---");
        panic!("assertion failed");
    }
}
