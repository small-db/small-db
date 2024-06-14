use std::thread;

use log::debug;
use rand::Rng;
use small_db::{
    btree::{buffer_pool::BufferPool, table::BTreeTableSearchIterator},
    concurrent_status::{ConcurrentStatus, Permission},
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

// Insert one tuple into the table
fn inserter(
    column_count: usize,
    table_rc: &Pod<BTreeTable>,
    s: &crossbeam::channel::Sender<Tuple>,
) {
    let mut rng = rand::thread_rng();
    let insert_value = rng.gen_range(i64::MIN, i64::MAX);

    let tx = Transaction::new();

    let tuple = new_int_tuples(insert_value, column_count, &tx);
    table_rc.rl().insert_tuple(&tx, &tuple).unwrap();
    tx.commit().unwrap();

    s.send(tuple).unwrap();
}

// Delete a random tuple from the table
fn deleter(table_rc: &Pod<BTreeTable>, r: &crossbeam::channel::Receiver<Tuple>) {
    let tuple = r.recv().unwrap();

    let predicate = Predicate::new(table_rc.rl().key_field, Op::Equals, &tuple.get_cell(0));

    // let tx = Transaction::new();
    // let table = table_rc.rl();
    // table.delete_tuples(&tx, &predicate).unwrap();
    // tx.commit().unwrap();

    let tx = Transaction::new();
    let table = table_rc.rl();
    let mut iter = BTreeTableSearchIterator::new(&tx, &table, &predicate);
    let tuple = iter.next().unwrap();
    table.delete_tuple(&tx, &tuple).unwrap();
    tx.commit().unwrap();
}

/// Doing lots of inserts and deletes simultaneously, this test aims to test the
/// correctness of the B+ tree implementation under concurrent environment.
///
/// Furthermore, this test also requires a fine-grained locking meachanism to be
/// implemented, the test will fail with timeout-error otherwise.
#[test]
fn test_concurrent() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    // Create a B+ tree with 2 pages in the first tier; the second and the third
    // tier are packed. (Which means the page spliting is imminent)
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

    // now insert some random tuples
    let (sender, receiver) = crossbeam::channel::unbounded();

    if let Err(e) = table.check_integrity(true) {
        table.draw_tree(-1);
        e.show_backtrace();
        panic!();
    }

    // test 1:
    // insert 1000 tuples, and make sure the tuple count is correct
    {
        let mut insert_threads = vec![];
        for _ in 0..1000 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let handle = thread::spawn(move || inserter(column_count, &local_table, &local_sender));
            insert_threads.push(handle);
        }
        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }

        assert_eq!(table.tuples_count(), row_count + 1000);
    }

    if let Err(e) = table.check_integrity(true) {
        table.draw_tree(-1);
        e.show_backtrace();
        panic!();
    }

    debug!("tuple count: {}", table.tuples_count());

    // test 2:
    // insert and delete tuples at the same time, make sure the tuple count is
    // correct, and the is no conflict between threads
    {
        let mut threads = vec![];
        for _ in 0..1000 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let insert_worker =
                thread::spawn(move || inserter(column_count, &local_table, &local_sender));
            threads.push(insert_worker);

            // thread local copies
            let local_table = table_pod.clone();
            let local_receiver = receiver.clone();

            let delete_worker = thread::spawn(move || deleter(&local_table, &local_receiver));
            threads.push(delete_worker);
        }
        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }

        table.draw_tree(3);
        table.check_integrity(true).unwrap();

        debug!("tuple count: {}", table.tuples_count());
        assert_eq!(table.tuples_count(), row_count + 1000);
        // assert_eq!(table.tuples_count(), row_count);
    }

    // return;

    // test 3:
    // insert and delete some tuples, make sure there is not too much pages created
    // during the process
    {
        let page_count_marker = table.pages_count();

        // delete a bunch of tuples
        let mut threads = vec![];
        for _ in 0..10 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_receiver = receiver.clone();

            let handle = thread::spawn(move || deleter(&local_table, &local_receiver));
            threads.push(handle);
        }

        // wait for all threads to finish, and make sure the tuple count is correct
        for handle in threads {
            handle.join().unwrap();
        }
        assert_eq!(table_pod.rl().tuples_count(), row_count + 1000 - 10);

        // insert a bunch of random tuples again
        let mut threads = vec![];
        for _ in 0..10 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let handle = thread::spawn(move || inserter(column_count, &local_table, &local_sender));
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }

        assert_eq!(table_pod.rl().tuples_count(), row_count + 1000);
        assert!(table_pod.rl().pages_count() < page_count_marker + 20);

        drop(sender);
    }

    // test 4:
    // look for all remained tuples and make sure we can find them
    {
        let tx = Transaction::new();
        for tuple in receiver.iter() {
            let predicate = Predicate::new(table.key_field, Op::Equals, &tuple.get_cell(0));
            let mut it = BTreeTableSearchIterator::new(&tx, &table_pod.rl(), &predicate);
            assert!(it.next().is_some());
        }
        tx.commit().unwrap();
        table_pod.rl().check_integrity(true).unwrap();
    }
}

#[test]
fn test_concurrent_page_access() {
    setup();

    // Set a short timeout for the test
    ConcurrentStatus::set_timeout(1);

    let table_pod = new_random_btree_table(2, 1, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let table = table_pod.rl();

    let write_tx = Transaction::new();
    let pid = table.get_root_pid(&write_tx);
    let page = BufferPool::get_leaf_page(&write_tx, Permission::ReadWrite, &pid);
    assert!(page.is_ok());

    // now using a read-only transaction to access the page, the result should be
    // timeout
    let read_tx = Transaction::new();
    let page = BufferPool::get_leaf_page(&read_tx, Permission::ReadOnly, &pid);
    assert!(page.is_err());
}
