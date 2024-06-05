use small_db::{btree::table::BTreeTableIterator, transaction::Transaction, BTreeTable};

use super::new_int_tuples;

pub fn delete_tuples(table: &BTreeTable, count: usize) {
    let tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..count {
        table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
    }
    tx.commit().unwrap();
}

pub fn insert_tuples(table: &BTreeTable, count: usize) {
    let tx = Transaction::new();
    for value in 0..count {
        let tuple = new_int_tuples(value as i64, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();
    }
    tx.commit().unwrap();
}

pub fn insert_row(table: &BTreeTable, tx: &Transaction, key: i64) {
    let tuple = new_int_tuples(key, 2, tx);
    table.insert_tuple(tx, &tuple).unwrap();
}
