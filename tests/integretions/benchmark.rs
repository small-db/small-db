use std::{env, thread};

use log::info;
use small_db::utils::HandyRwLock;

use crate::test_utils::{insert_random, new_random_btree_table, setup, TreeLayout};
use small_db::Database;

#[test]
#[cfg(feature = "benchmark")]
fn test_insert_parallel() {
    setup();

    let action_per_thread = env::var("ACTION_PER_THREAD")
        .unwrap_or("100".to_string())
        .parse::<usize>()
        .unwrap();
    let thread_count = env::var("THREAD_COUNT")
        .unwrap_or("10".to_string())
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

    {
        let mut log_manager = Database::mut_log_manager();
        Database::mut_buffer_pool().flush_all_pages(&mut log_manager);
    }

    table.draw_tree(-1);

    let duration = start.elapsed();
    let expect_rows = thread_count * action_per_thread;
    info!("{} insertion threads took: {:?}", thread_count, duration);
    info!("ms:{:?}", duration.as_millis());
    Database::mut_buffer_pool().clear();
    info!(
        "table.tuples_count(): {:?}, expect: {:?}",
        table.tuples_count(),
        expect_rows,
    );
    assert!(table.tuples_count() == expect_rows);
}
