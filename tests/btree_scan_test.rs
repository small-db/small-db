use log::info;
use rand::Rng;
use simple_db_rust::btree::{
    buffer_pool::BufferPool, table::BTreeTableIterator,
};

mod common;

fn test_scan(rows_list: Vec<usize>, column_count: Vec<usize>) {
    let mut rng = rand::thread_rng();
    for rows in rows_list.iter() {
        for columns in column_count.iter() {
            let mut int_tuples: Vec<Vec<i32>> = Vec::new();
            let key_field = rng.gen_range(0, columns);
            let table_rc = common::create_random_btree_table(
                *columns,
                *rows,
                Some(&mut int_tuples),
                key_field,
                false,
            );
            let table = table_rc.borrow();
            let mut it = BTreeTableIterator::new(&table);
            validate_scan(&mut it, &int_tuples);

            // TODO: find a better solution
            BufferPool::global().clear();
        }
    }
}

fn validate_scan(it: &mut BTreeTableIterator, int_tuples: &Vec<Vec<i32>>) {
    for (i, tuple) in it.enumerate() {
        for (j, f) in tuple.fields.iter().enumerate() {
            assert_eq!(f.value, int_tuples[i][j]);
        }
    }
}

#[test]
fn test_small() {
    common::setup();

    let column_count_list = vec![1, 2, 3, 4, 5];
    let row_count_list =
        vec![0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + 521];
    test_scan(row_count_list, column_count_list);
}
