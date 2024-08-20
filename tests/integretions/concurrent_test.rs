use std::thread;

use small_db::{
    btree::{buffer_pool::BufferPool, table::BTreeTableSearchIterator},
    storage::tuple::Tuple,
    transaction::{Permission, Transaction},
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, Op, Predicate,
};

use crate::test_utils::{
    insert_random, internal_children_cap, leaf_records_cap, new_int_tuples, new_random_btree_table,
    setup, TreeLayout,
};

// Delete a tuple from the table.
fn deleter(table_rc: &Pod<BTreeTable>, r: &crossbeam::channel::Receiver<Tuple>) {
    let tuple = r.recv().unwrap();

    let predicate = Predicate::new(table_rc.rl().key_field, Op::Equals, &tuple.get_cell(0));

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
    let table_rc = new_random_btree_table(
        column_count,
        row_count,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let table = table_rc.rl();

    // now insert some random tuples
    let (sender, receiver) = crossbeam::channel::unbounded();

    // test 1:
    // insert 1000 tuples, and make sure the tuple count is correct
    {
        let mut insert_threads = vec![];
        for _ in 0..1000 {
            // thread local copies
            let local_table = table_rc.clone();
            let local_sender = sender.clone();

            let handle = thread::spawn(move || {
                insert_random(local_table, 1, column_count, Some(&local_sender))
            });
            insert_threads.push(handle);
        }
        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }

        table.check_integrity();
        assert_eq!(table.tuples_count(), row_count + 1000);
    }

    // test 2:
    // insert and delete tuples at the same time, make sure the tuple count is
    // correct, and the is no conflict between threads
    {
        let mut threads = vec![];
        for _ in 0..200 {
            // thread local copies
            let local_table = table_rc.clone();
            let local_sender = sender.clone();

            let insert_worker = thread::spawn(move || {
                insert_random(local_table, 1, column_count, Some(&local_sender))
            });
            threads.push(insert_worker);

            // thread local copies
            let local_table = table_rc.clone();
            let local_receiver = receiver.clone();

            let delete_worker = thread::spawn(move || deleter(&local_table, &local_receiver));
            threads.push(delete_worker);
        }
        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }

        table.check_integrity();
        assert_eq!(table.tuples_count(), row_count + 1000);
    }

    // test 3:
    // insert and delete some tuples, make sure there is not too much pages created
    // during the process
    {
        let page_count_marker = table.pages_count();

        // delete a bunch of tuples
        let mut threads = vec![];
        for _ in 0..10 {
            // thread local copies
            let local_table = table_rc.clone();
            let local_receiver = receiver.clone();

            let handle = thread::spawn(move || deleter(&local_table, &local_receiver));
            threads.push(handle);
        }

        // wait for all threads to finish, and make sure the tuple count is correct
        for handle in threads {
            handle.join().unwrap();
        }
        assert_eq!(table_rc.rl().tuples_count(), row_count + 1000 - 10);

        // insert a bunch of random tuples again
        let mut threads = vec![];
        for _ in 0..10 {
            // thread local copies
            let local_table = table_rc.clone();
            let local_sender = sender.clone();

            let handle = thread::spawn(move || {
                insert_random(local_table, 1, column_count, Some(&local_sender))
            });
            threads.push(handle);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }

        assert_eq!(table_rc.rl().tuples_count(), row_count + 1000);
        assert!(table_rc.rl().pages_count() < page_count_marker + 20);

        drop(sender);
    }

    // test 4:
    // look for all remained tuples and make sure we can find them
    {
        let tx = Transaction::new();
        for tuple in receiver.iter() {
            let predicate = Predicate::new(table.key_field, Op::Equals, &tuple.get_cell(0));
            let mut it = BTreeTableSearchIterator::new(&tx, &table_rc.rl(), &predicate);
            assert!(it.next().is_some());
        }
        tx.commit().unwrap();
        table.check_integrity();
    }
}

/// Assert two transactions cannot access the same page at the same time using
/// exclusive permission.
///
/// This test should be passed no matter what the latch mechanism is.
#[test]
fn test_concurrent_page_access() {
    setup();

    let table_rc = new_random_btree_table(2, 1, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let table = table_rc.rl();

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

/// Make sure we can handle lots of (1000+) concurrent insert operations.
/// 
/// TODO: this test is marked "benchmark" since it's too slow.
#[test]
#[cfg(feature = "benchmark")]
fn test_concurrent_insert() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    let table_rc = new_random_btree_table(2, 0, None, 0, TreeLayout::LastTwoEvenlyDistributed);

    let table = table_rc.rl();

    // insert and delete tuples at the same time, make sure the tuple count is
    // correct, and the is no conflict between threads
    let mut threads = vec![];
    for _ in 0..1000 {
        // thread local copies
        let local_table = table_rc.clone();

        let insert_worker = thread::spawn(move || inserter3(2, &local_table));
        threads.push(insert_worker);
    }
    // wait for all threads to finish
    for handle in threads {
        handle.join().unwrap();
    }

    assert_eq!(table.tuples_count(), 0);
}

// TODO: remove this function after we merged api "delete_tuple" and "delete_tuples"
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

/// Make sure we can handle lots of (1000+) concurrent delete operations.
#[test]
fn test_concurrent_delete() {
    // Use a small page size to speed up the test.
    BufferPool::set_page_size(1024);

    setup();

    let row_count = 0;
    let column_count = 2;
    let table_rc = new_random_btree_table(
        column_count,
        row_count,
        None,
        0,
        TreeLayout::LastTwoEvenlyDistributed,
    );

    let table = table_rc.rl();

    // now insert some random tuples
    let (sender, receiver) = crossbeam::channel::unbounded();
    let concurrency = 1000;
    insert_random(table_rc.clone(), concurrency, column_count, Some(&sender));

    {
        let mut threads = vec![];
        for _ in 0..concurrency {
            // thread local copies
            let local_table = table_rc.clone();
            let local_receiver = receiver.clone();

            let delete_worker = thread::spawn(move || deleter(&local_table, &local_receiver));
            threads.push(delete_worker);
        }
        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }

        table.check_integrity();
        assert_eq!(table.tuples_count(), row_count);
    }
}
