use small_db::{
    btree::table::BTreeTableIterator, storage::tuple::Tuple, transaction::Transaction, BTreeTable,
};

pub fn delete_tuples(table: &BTreeTable, count: usize) {
    let mut tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..count {
        table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
    }
    tx.commit().unwrap();
}

pub fn insert_tuples(table: &BTreeTable, count: usize) {
    let mut tx = Transaction::new();
    for value in 0..count {
        let tuple = Tuple::new_int_tuples(value as i64, 2);
        table.insert_tuple(&mut tx, &tuple).unwrap();
    }
    tx.commit().unwrap();
}
