use std::thread;

use log::{debug, info};
use rand::prelude::*;
use small_db::{
    btree::{page_cache::PageCache, table::BTreeTableSearchIterator},
    storage::tuple::Tuple,
    transaction::Transaction,
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, Database, Predicate,
};

use crate::test_utils::{
    assert_true, internal_children_cap, leaf_records_cap,
    new_random_btree_table, setup, TreeLayout,
};

// Insert one tuple into the table
fn inserter(
    column_count: usize,
    table_pod: &Pod<BTreeTable>,
    s: &crossbeam::channel::Sender<Tuple>,
) {
    let mut rng = rand::thread_rng();
    let insert_value = rng.gen_range(i64::MIN, i64::MAX);
    let tuple = Tuple::new_int_tuples(insert_value, column_count);

    let tx = Transaction::new();
    debug!("{} prepare to insert", tx);
    if let Err(e) = table_pod.rl().insert_tuple(&tx, &tuple) {
        table_pod.rl().draw_tree(-1);
        panic!("Error inserting tuple: {}", e);
    }
    debug!("{} insert done", tx);
    tx.commit().unwrap();

    s.send(tuple).unwrap();
}

// Delete a random tuple from the table
fn deleter(
    table_pod: &Pod<BTreeTable>,
    r: &crossbeam::channel::Receiver<Tuple>,
) {
    let cs = Database::concurrent_status();
    debug!("concurrent_status: {:?}", cs);

    let tuple = r.recv().unwrap();
    let predicate =
        Predicate::new(small_db::Op::Equals, &tuple.get_cell(0));

    let tx = Transaction::new();
    let table = table_pod.rl();

    debug!("{} prepare to delete", tx);
    let mut it =
        BTreeTableSearchIterator::new(&tx, &table, &predicate);
    let _target = it.next().unwrap();
    // table.delete_tuple(&tx, &target).unwrap();

    tx.commit().unwrap();
}

// Test that doing lots of inserts and deletes in multiple threads
// works.
// #[test]
fn test_big_table() {
    setup();

    // For this test we will decrease the size of the Buffer Pool
    // pages.
    PageCache::set_page_size(1024);

    // Create a B+ tree with 2 nodes in the first tier; the second and
    // the third tier are packed.
    let row_count = 2 * internal_children_cap() * leaf_records_cap();
    let column_count = 2;
    let table_pod = new_random_btree_table(
        column_count,
        row_count,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );
    let table = table_pod.rl();

    let cs = Database::concurrent_status();
    debug!("Concurrent status: {:?}", cs);

    debug!("Start insertion in multiple threads...");

    // now insert some random tuples
    let (sender, receiver) = crossbeam::channel::unbounded();
    thread::scope(|s| {
        let mut insert_threads = vec![];
        for _ in 0..1000 {
            let handle = s.spawn(|| {
                inserter(column_count, &table_pod, &sender)
            });
            insert_threads.push(handle);
        }

        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }
    });

    assert_true(
        table_pod.rl().tuples_count() == row_count + 1000,
        &table,
    );

    // now insert and delete tuples at the same time
    thread::scope(|s| {
        let mut threads = vec![];
        for _ in 0..1000 {
            let handle = s.spawn(|| {
                inserter(column_count, &table_pod, &sender)
            });
            threads.push(handle);

            let handle = s.spawn(|| deleter(&table_pod, &receiver));
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });

    info!(
        "row_count: {}, tuples_count: {}",
        row_count,
        table.tuples_count()
    );
    return;

    assert_true(
        table_pod.rl().tuples_count() == row_count + 1000,
        &table,
    );

    let page_count_marker = table_pod.rl().pages_count();

    // now delete a bunch of tuples
    thread::scope(|s| {
        let mut threads = vec![];
        for _ in 0..10 {
            let handle = s.spawn(|| deleter(&table_pod, &receiver));
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });
    assert_eq!(table_pod.rl().tuples_count(), row_count + 1000 - 10);

    // now insert a bunch of random tuples again
    thread::scope(|s| {
        let mut threads = vec![];
        for _ in 0..10 {
            let handle = s.spawn(|| {
                inserter(column_count, &table_pod, &sender)
            });
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });
    assert_eq!(table_pod.rl().tuples_count(), row_count + 1000);
    assert!(table_pod.rl().pages_count() < page_count_marker + 20);

    drop(sender);

    // look for all tuples and make sure we can find them
    let tx = Transaction::new();
    for tuple in receiver.iter() {
        let predicate =
            Predicate::new(small_db::Op::Equals, &tuple.get_cell(0));
        let mut it = BTreeTableSearchIterator::new(
            &tx,
            &table_pod.rl(),
            &predicate,
        );
        assert!(it.next().is_some());
    }
    tx.commit().unwrap();
    table_pod.rl().check_integrity(true);
}
