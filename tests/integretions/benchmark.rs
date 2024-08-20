use std::{env, thread};

use log::info;
use rand::Rng;
use small_db::{btree::buffer_pool::BufferPool, transaction::Transaction, utils::HandyRwLock};

use crate::test_utils::{insert_random, new_int_tuples, new_random_btree_table, setup, TreeLayout};

// TODO: this test doesn't work. (deadlocks)
#[test]
#[cfg(feature = "benchmark")]
fn test_speed() {
    // Use a longer timeout for "benchmark" tests.
    use small_db::transaction::ConcurrentStatus;
    if cfg!(feature = "benchmark") {
        ConcurrentStatus::set_timeout(1000);
    }

    let action_per_thread = env::var("ACTION_PER_THREAD")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let thread_count = env::var("THREAD_COUNT").unwrap().parse::<usize>().unwrap();

    setup();

    // Create an empty B+ tree
    let column_count = 2;
    let table_rc = new_random_btree_table(
        column_count,
        0,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let table = table_rc.rl();

    let start = std::time::Instant::now();
    // run insert threads
    {
        let mut insert_threads = vec![];
        for _ in 0..thread_count {
            // thread local copies
            let local_table = table_rc.clone();

            let handle = thread::spawn(move || {
                insert_random(local_table, action_per_thread, column_count, None)
            });
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

#[test]
#[cfg(feature = "benchmark")]
fn test_insert_benchmark() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    let row_count = 0;
    let table_rc = new_random_btree_table(2, row_count, None, 0, TreeLayout::EvenlyDistributed);
    let table = table_rc.rl();

    // run insert, find the performance bottleneck
    let mut rng = rand::thread_rng();

    for _ in 0..3000 {
        let tx = Transaction::new();

        let insert_value = rng.gen_range(0, i64::MAX);
        let tuple = new_int_tuples(insert_value, 2, &tx);
        table.insert_tuple(&tx, &tuple).unwrap();

        tx.commit().unwrap();
    }
}
