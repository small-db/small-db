use rand::Rng;
use small_db::{
    btree::table::BTreeTableIterator, transaction::Transaction,
    utils::HandyRwLock, Database,
};

use crate::test_utils::{new_random_btree_table, setup, TreeLayout};

fn test_scan(rows: usize, columns: usize) {
    let tx = Transaction::new();
    let mut rng = rand::thread_rng();
    let mut int_tuples: Vec<Vec<i32>> = Vec::new();
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

    // TODO: find a better solution
    Database::mut_page_cache().clear();
    Database::concurrent_status().clear();
}

fn validate_scan(
    it: &mut BTreeTableIterator,
    int_tuples: &Vec<Vec<i32>>,
) {
    for (i, tuple) in it.enumerate() {
        for (j, f) in tuple.cells.iter().enumerate() {
            assert_eq!(f.value, int_tuples[i][j]);
        }
    }
}

#[test]
fn test_small() {
    setup();

    let column_count_list: Vec<usize> = vec![1, 2, 3, 4, 5];
    let row_count_list: Vec<usize> =
        vec![0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + 521];

    for rows in row_count_list.iter() {
        for columns in column_count_list.iter() {
            test_scan(*rows, *columns);
        }
    }
}

// not needed for now
#[test]
fn test_rewind() {}

// not needed for now
#[test]
fn test_rewind_predicates() {}

// not needed for now
#[test]
fn test_read_page() {}
