use std::thread;

use log::debug;
use rand::Rng;
use small_db::{
    btree::{buffer_pool::BufferPool, table::BTreeTableSearchIterator},
    storage::tuple::Tuple,
    transaction::Transaction,
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, Database, Op, Predicate,
};

use crate::test_utils::{
    assert_true, internal_children_cap, leaf_records_cap, new_random_btree_table, setup, TreeLayout,
};

// Insert one tuple into the table
fn inserter(
    tx_id: u64,
    column_count: usize,
    table_rc: &Pod<BTreeTable>,
    s: &crossbeam::channel::Sender<Tuple>,
) {
    let mut rng = rand::thread_rng();
    let insert_value = rng.gen_range(i64::MIN, i64::MAX);
    let tuple = Tuple::new_int_tuples(insert_value, column_count);

    let tx = Transaction::new();
    tx.start().unwrap();

    table_rc.rl().insert_tuple(&tx, &tuple).unwrap();
    tx.commit().unwrap();

    s.send(tuple).unwrap();
}

// Delete a random tuple from the table
fn deleter(tx_id: u64, table_rc: &Pod<BTreeTable>, r: &crossbeam::channel::Receiver<Tuple>) {
    let tuple = r.recv().unwrap();
    let predicate = Predicate::new(table_rc.rl().key_field, Op::Equals, &tuple.get_cell(0));

    let tx = Transaction::new_specific_id(tx_id);
    tx.start().unwrap();

    // let tx = Transaction::new();
    let table = table_rc.rl();
    let mut it = BTreeTableSearchIterator::new(&tx, &table, &predicate);

    // let target = it.next().unwrap();

    let search_result = it.next();
    if search_result.is_none() {
        debug!("tuple not found: {:?}", tuple);
        table.draw_tree(-1);
        table.check_integrity(true);

        Database::mut_log_manager().show_log_contents();

        panic!("tuple not found: {:?}", tuple);

        tx.commit().unwrap();
        return;
    }
    let target = search_result.unwrap();

    table.delete_tuple(&tx, &target).unwrap();

    tx.commit().unwrap();
}

#[test]
/// Doing lots of inserts and deletes simultaneously, this test aims to test the
/// correctness of the B+ tree implementation under concurrent environment.
///
/// Furthermore, this test also requires a fine-grained locking meachanism to be
/// implemented, the test will fail with timeout-error otherwise.
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

    let insert_count: usize = 1000;

    thread::scope(|s| {
        let mut insert_threads = vec![];
        for i in 0..insert_count {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let handle = thread::Builder::new()
                .name(format!("thread-{}", i).to_string())
                .spawn_scoped(s, move || {
                    inserter(i as u64 + 1000, column_count, &local_table, &local_sender)
                })
                .unwrap();

            insert_threads.push(handle);
        }

        // wait for all threads to finish
        for handle in insert_threads {
            handle.join().unwrap();
        }
    });

    assert_true(
        table_pod.rl().tuples_count() == row_count + insert_count,
        &table,
    );
    return;

    // assert_true(table_pod.rl().tuples_count() == row_count + 1000, &table);

    // now insert and delete tuples at the same time
    thread::scope(|s| {
        let mut threads = vec![];
        for i in 0..200 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let insert_worker = thread::Builder::new()
                .name(format!("thread-insert-{}", i).to_string())
                .spawn_scoped(s, move || {
                    inserter(i, column_count, &local_table, &local_sender)
                })
                .unwrap();
            threads.push(insert_worker);

            // thread local copies
            let tx_id = i + 10000;
            let local_table = table_pod.clone();
            let local_receiver = receiver.clone();

            let delete_worker = thread::Builder::new()
                .name(format!("thread-delete-{}", i).to_string())
                .spawn_scoped(s, move || deleter(tx_id, &local_table, &local_receiver))
                .unwrap();
            threads.push(delete_worker);
        }

        // wait for all threads to finish
        for handle in threads {
            handle.join().unwrap();
        }
    });

    return;

    assert_true(table_pod.rl().tuples_count() == row_count + 1000, &table);

    let page_count_marker = table_pod.rl().pages_count();

    // now delete a bunch of tuples
    thread::scope(|s| {
        let mut threads = vec![];
        for i in 0..10 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_receiver = receiver.clone();

            let handle = s.spawn(move || deleter(i, &local_table, &local_receiver));
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
        for i in 0..10 {
            // thread local copies
            let local_table = table_pod.clone();
            let local_sender = sender.clone();

            let handle = s.spawn(move || inserter(i, column_count, &local_table, &local_sender));
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

    // look for all remained tuples and make sure we can find them
    let mut tx = Transaction::new();
    for tuple in receiver.iter() {
        let predicate = Predicate::new(table.key_field, Op::Equals, &tuple.get_cell(0));
        let mut it = BTreeTableSearchIterator::new(&tx, &table_pod.rl(), &predicate);
        assert!(it.next().is_some());
    }
    tx.commit().unwrap();
    table_pod.rl().check_integrity(true);
}
