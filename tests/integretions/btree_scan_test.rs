use rand::Rng;
use small_db::{
    btree::table::BTreeTableIterator, storage::tuple::Cell, transaction::Transaction,
    utils::HandyRwLock,
};

use crate::test_utils::{new_random_btree_table, setup, TreeLayout};

fn test_scan(rows: usize, columns: usize) {
    setup();

    let mut tx = Transaction::new();
    let mut rng = rand::thread_rng();
    let mut int_tuples = Vec::new();

    // TODO: remove this
    let key_field = rng.gen_range(0, columns);

    let table_rc = new_random_btree_table(
        columns,
        rows,
        Some(&mut int_tuples),
        key_field,
        TreeLayout::Naturally,
    );
    let table = table_rc.rl();
    let mut it = BTreeTableIterator::new(&tx, &table);
    validate_scan(&mut it, &int_tuples);
}

fn validate_scan(it: &mut BTreeTableIterator, int_tuples: &Vec<Vec<Cell>>) {
    for (i, tuple) in it.enumerate() {
        for (j, cell) in tuple.get_cells().iter().enumerate() {
            assert_eq!(cell, &int_tuples[i][j]);
        }
    }
}

#[test]
fn test_small() {
    setup();

    let column_count_list: Vec<usize> = vec![1, 2, 3, 4, 5];
    let row_count_list: Vec<usize> = vec![0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + 521];

    for rows in row_count_list.iter() {
        for columns in column_count_list.iter() {
            test_scan(*rows, *columns);
        }
    }
}
