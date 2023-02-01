use small_db::{
    btree::table::BTreeTableIterator, transaction::Transaction,
    BTreeTable, Tuple,
};

use super::leaf_records_cap;

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
        let tuple = Tuple::new_btree_tuple(value as i32, 2);
        table.insert_tuple(&tx, &tuple).unwrap();
    }
    tx.commit().unwrap();
}
