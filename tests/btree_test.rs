mod common;
use std::{
    thread::{self, sleep},
    time::Duration,
};

use common::TreeLayout;
use log::debug;
use rand::prelude::*;
use small_db::{
    btree::{buffer_pool::BufferPool, table::BTreeTableSearchIterator},
    transaction::Transaction,
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, Predicate, Tuple, Unique,
};

// Insert one tuple into the table
fn inserter(
    column_count: usize,
    table_pod: &Pod<BTreeTable>,
    s: &crossbeam::channel::Sender<Tuple>,
) {
    let mut rng = rand::thread_rng();
    let insert_value = rng.gen_range(i32::MIN, i32::MAX);
    let tuple = Tuple::new_btree_tuple(insert_value, column_count);

    let tx = Transaction::new();
    debug!("{} prepare to insert", tx);
    if let Err(e) = table_pod.rl().insert_tuple(&tx, &tuple) {
        table_pod.rl().draw_tree(-1);
        panic!("Error inserting tuple: {}", e);
    }
    debug!("{} insert done", tx);
    s.send(tuple).unwrap();
    tx.commit().unwrap();
}

// Delete a random tuple from the table
fn deleter(
    table_pod: &Pod<BTreeTable>,
    r: &crossbeam::channel::Receiver<Tuple>,
) {
    let cs = Unique::concurrent_status();
    debug!("concurrent_status: {:?}", cs);

    let tuple = r.recv().unwrap();
    let predicate = Predicate::new(small_db::Op::Equals, tuple.get_field(0));
    let tx = Transaction::new();
    let table = table_pod.rl();

    debug!("{} prepare to delete", tx);
    let mut it = BTreeTableSearchIterator::new(&tx, &table, predicate);
    let target = it.next().unwrap();
    table.delete_tuple(&tx, &target).unwrap();

    tx.commit().unwrap();
}

// Test that doing lots of inserts and deletes in multiple threads works.
#[test]
fn test_big_table() {
    let _ctx = common::setup();

    // For this test we will decrease the size of the Buffer Pool pages.
    BufferPool::set_page_size(1024);

    // This should create a B+ tree with a packed second tier of internal pages
    // and packed third tier of leaf pages.
    //
    // (124 entries per internal/leaf page, 125 children per internal page)
    //
    // 1st tier: 1 internal page
    // 2nd tier: 2 internal pages (2 * 125 = 250 children)
    // 3rd tier: 250 leaf pages (250 * 124 = 31,000 entries)
    debug!("Creating large random B+ tree...");
    let columns = 2;
    let table_pod = common::create_random_btree_table(
        columns,
        31000,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let cs = Unique::concurrent_status();
    debug!("Concurrent status: {:?}", cs);

    debug!("Start insertion in multiple threads...");

    // now insert some random tuples
    let (sender, receiver) = crossbeam::channel::unbounded();
    thread::scope(|s| {
        let mut insert_threads = vec![];
        for _ in 0..200 {
            let handle = s.spawn(|| inserter(columns, &table_pod, &sender));
            // The first few inserts will cause pages to split so give them a
            // little more time to avoid too many deadlock situations.
            sleep(Duration::from_millis(10));
            insert_threads.push(handle);
        }

        for _ in 0..800 {
            let handle = s.spawn(|| inserter(columns, &table_pod, &sender));
            insert_threads.push(handle);
        }

        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }
    });

    debug!("Concurrent status: {:?}", cs);

    assert_eq!(table_pod.rl().tuples_count(), 31000 + 1000);

    // now insert and delete tuples at the same time
    thread::scope(|s| {
        let mut threads = vec![];
        for _ in 0..1000 {
            let handle = s.spawn(|| inserter(columns, &table_pod, &sender));
            threads.push(handle);

            let handle = s.spawn(|| deleter(&table_pod, &receiver));
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });
    assert_eq!(table_pod.rl().tuples_count(), 31000 + 1000);
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
    assert_eq!(table_pod.rl().tuples_count(), 31000 + 1000 - 10);

    // now insert a bunch of random tuples again
    thread::scope(|s| {
        let mut threads = vec![];
        for _ in 0..10 {
            let handle = s.spawn(|| inserter(columns, &table_pod, &sender));
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });
    assert_eq!(table_pod.rl().tuples_count(), 31000 + 1000);
    assert!(table_pod.rl().pages_count() < page_count_marker + 20);

    drop(sender);

    // look for all tuples and make sure we can find them
    let tx = Transaction::new();
    for tuple in receiver.iter() {
        let predicate =
            Predicate::new(small_db::Op::Equals, tuple.get_field(0));
        let mut it =
            BTreeTableSearchIterator::new(&tx, &table_pod.rl(), predicate);
        assert!(it.next().is_some());
    }
    tx.commit().unwrap();
    table_pod.rl().check_integrity(true);
}
