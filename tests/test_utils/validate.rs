use log::error;
use small_db::{
    btree::table::BTreeTableSearchIterator, storage::tuple::IntCell,
    transaction::Transaction, BTreeTable, Database, Op, Predicate,
};

pub fn key_present(
    tx: &Transaction,
    table: &BTreeTable,
    key: i32,
) -> bool {
    let predicate =
        Predicate::new(small_db::Op::Equals, IntCell::new(key));
    let mut it = BTreeTableSearchIterator::new(tx, &table, predicate);
    it.next().is_some()
}

// Search for a key in the table and return the number of records.
pub fn search_key(
    table: &BTreeTable,
    tx: &Transaction,
    key: i32,
) -> usize {
    let predicate = Predicate::new(Op::Equals, IntCell::new(key));
    let it = BTreeTableSearchIterator::new(&tx, &table, predicate);
    return it.count();
}

pub fn assert_true(predicate: bool, table: &BTreeTable) {
    if !predicate {
        error!("--- assertion failed, debug_info start ---");
        Database::log_file().show_log_contents();
        // panic!("assertion failed");
        table.draw_tree(-1);
        table.check_integrity(true);
        error!("--- assertion failed, debug_info end ---");
        panic!("assertion failed");
    }
}
