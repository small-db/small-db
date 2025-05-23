use std::{env, thread};

use log::info;
use small_db::{utils::HandyRwLock, Database};

use crate::test_utils::{insert_random, new_random_btree_table, setup, TreeLayout};

#[test]
#[cfg(feature = "benchmark")]
// action_per_thread: 1000
// threads_count: 100
// best time: 5.74s.
fn test_insert_parallel() {
    setup();

    let action_per_thread = env::var("ACTION_PER_THREAD")
        .unwrap_or("1000".to_string())
        .parse::<usize>()
        .unwrap();
    let threads_count = env::var("THREADS_COUNT")
        .unwrap_or("100".to_string())
        .parse::<usize>()
        .unwrap();

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
    {
        let mut insert_threads = vec![];
        for _ in 0..threads_count {
            let local_table = table_rc.clone();

            let handle = thread::spawn(move || {
                insert_random(local_table, action_per_thread, column_count, None)
            });
            insert_threads.push(handle);
        }
        for handle in insert_threads {
            handle.join().unwrap();
        }
    }

    {
        let mut log_manager = Database::mut_log_manager();
        Database::mut_buffer_pool().flush_all_pages(&mut log_manager);
    }

    let duration = start.elapsed();
    let expect_rows = threads_count * action_per_thread;
    info!("{} insertion threads took: {:?}", threads_count, duration);
    info!("ms:{:?}", duration.as_millis());

    Database::reset();

    info!(
        "table.tuples_count(): {:?}, expect: {:?}",
        table.tuples_count(),
        expect_rows,
    );
    assert!(table.tuples_count() == expect_rows);
}
