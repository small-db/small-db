use log::error;
use small_db::{
    btree::table::BTreeTableSearchIterator, storage::tuple::Cell, transaction::Transaction,
    BTreeTable, Database, Op, Predicate,
};

// Search for a key in the table and return the number of records.
pub fn search_key(table: &BTreeTable, tx: &Transaction, key: &Cell) -> usize {
    let predicate = Predicate::new(table.key_field, Op::Equals, key);
    let it = BTreeTableSearchIterator::new(&tx, &table, &predicate);
    return it.count();
}

pub fn assert_true(predicate: bool, table: &BTreeTable) {
    if !predicate {
        error!("--- assertion failed, debug_info start ---");
        Database::mut_log_manager().show_log_contents();
        table.draw_tree(-1);
        table.check_integrity();
        error!("--- assertion failed, debug_info end ---");
        panic!("assertion failed");
    }
}
