use std::thread;

use log::debug;
use rand::Rng;
use small_db::{
    btree::{buffer_pool::BufferPool, table::BTreeTableSearchIterator},
    storage::tuple::Tuple,
    transaction::Transaction,
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, Op, Predicate,
};

use crate::test_utils::{
    internal_children_cap, leaf_records_cap, new_int_tuples, new_random_btree_table, setup,
    TreeLayout,
};

#[test]
#[cfg(feature = "benchmark")]
fn test_speed() {
    use std::env;

    use log::info;

    let action_per_thread = env::var("ACTION_PER_THREAD")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let thread_count = env::var("THREAD_COUNT").unwrap().parse::<usize>().unwrap();

    setup();

    // Create an empty B+ tree
    let column_count = 2;
    let table_pod = new_random_btree_table(
        column_count,
        0,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let table = table_pod.rl();

    let start = std::time::Instant::now();
    // run insert threads
    {
        let mut insert_threads = vec![];
        for _ in 0..thread_count {
            // thread local copies
            let local_table = table_pod.clone();

            let handle =
                thread::spawn(move || inserter2(action_per_thread, column_count, &local_table));
            insert_threads.push(handle);
        }
        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }
    }
    let duration = start.elapsed();
    let total_rows = thread_count * action_per_thread;
    info!("{} insertion threads took: {:?}", thread_count, duration);
    info!("ms:{:?}", duration.as_millis());
    info!(
        "table.tuples_count(): {:?}, total_rows: {:?}",
        table.tuples_count(),
        total_rows,
    );
    assert!(table.tuples_count() == total_rows);
}

// Insert one tuple into the table
fn inserter3(column_count: usize, table_rc: &Pod<BTreeTable>) {
    let table = table_rc.rl();

    let tx = Transaction::new();
    let tuple = new_int_tuples(tx.get_id() as i64, column_count, &tx);
    table.insert_tuple(&tx, &tuple).unwrap();
    tx.commit().unwrap();

    let predicate = Predicate::new(table.key_field, Op::Equals, &tuple.get_cell(0));
    table.delete_tuples(&tx, &predicate).unwrap();
    tx.commit().unwrap();
}

#[test]
fn test_concurrent_simplified() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    let table_pod = new_random_btree_table(2, 0, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let table = table_pod.rl();

    // insert and delete tuples at the same time, make sure the tuple count is
    // correct, and the is no conflict between threads
    let mut threads = vec![];
    for _ in 0..1000 {
        // thread local copies
        let local_table = table_pod.clone();

        let insert_worker = thread::spawn(move || inserter3(2, &local_table));
        threads.push(insert_worker);
    }
    // wait for all threads to finish
    for handle in threads {
        handle.join().unwrap();
    }

    assert_eq!(table.tuples_count(), 0);
}
