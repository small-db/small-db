use std::sync::{Arc, RwLock};

use rand::Rng;
use small_db::{
    btree::table::BTreeTableIterator, storage::tuple::Tuple, transaction::Transaction,
    utils::HandyRwLock, BTreeTable, Database,
};

use super::new_int_tuples;

pub fn delete_tuples(table: &BTreeTable, count: usize) {
    let tx = Transaction::new();
    let mut it = BTreeTableIterator::new(&tx, &table);
    for _ in 0..count {
        table.delete_tuple(&tx, &it.next().unwrap()).unwrap();
    }
    tx.commit().unwrap();

    table.delete_invisible_tuples().unwrap();
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

/// Insert random tuples into the table.
pub fn insert_random(
    table_rc: Arc<RwLock<BTreeTable>>,
    row_count: usize,
    column_count: usize,
    s: Option<&crossbeam::channel::Sender<Tuple>>,
) {
    let mut rng = rand::thread_rng();
    let table = table_rc.rl();

    let tx = Transaction::new();

    let tuples: Vec<Tuple> = (0..row_count)
        .map(|_| {
            let insert_value = rng.gen_range(i64::MIN, i64::MAX);
            new_int_tuples(insert_value, column_count, &tx)
        })
        .collect();

    for tuple in &tuples {
        table.crab_insert_tuple(&tx, &tuple).unwrap();
    }
    tx.commit().unwrap();
}
