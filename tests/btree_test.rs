mod common;
use std::{
    thread::{self, sleep, Scope},
    time::Duration,
};

use common::TreeLayout;
use log::debug;
use rand::prelude::*;
use simple_db_rust::{
    btree::buffer_pool::BufferPool, transaction::Transaction, types::Pod,
    utils::HandyRwLock, BTreeTable, Tuple,
};

fn inserter(column_count: usize, table_pod: &Pod<BTreeTable>) {
    let mut rng = rand::thread_rng();
    let insert_value = rng.gen_range(i32::MIN, i32::MAX);
    let tuple = Tuple::new_btree_tuple(insert_value, column_count);

    let tx = Transaction::new();
    debug!("{} prepare to insert", tx);
    if let Err(e) = table_pod.rl().insert_tuple(&tx, &tuple) {
        table_pod.rl().draw_tree(&tx, -1);
        panic!("Error inserting tuple: {}", e);
    }
    debug!("{} insert done", tx);
    tx.commit().unwrap();
}

// Test that doing lots of inserts and deletes in multiple threads works.
#[test]
fn test_big_table() {
    let _ = common::setup();

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

    debug!("Start insertion in multiple threads...");

    // now insert some random tuples
    thread::scope(|s| {
        let mut insert_threads = vec![];
        for _ in 0..200 {
            let handle = s.spawn(|| inserter(columns, &table_pod));
            // The first few inserts will cause pages to split so give them a little more time to avoid too many deadlock situations.
            sleep(Duration::from_millis(10));
            insert_threads.push(handle);
        }

        for _ in 0..800 {
            let handle = s.spawn(|| inserter(columns, &table_pod));
            insert_threads.push(handle);
        }

        for handle in insert_threads {
            handle.join().unwrap();
        }
    });

    // ArrayBlockingQueue<ArrayList<Integer>> insertedTuples = new
    // ArrayBlockingQueue<ArrayList<Integer>>(100000); insertedTuples.
    // addAll(tuples); assertEquals(31000, insertedTuples.size());
    // int size = insertedTuples.size();

    // // now insert some random tuples
    // System.out.println("Inserting tuples...");
    // ArrayList<BTreeInserter> insertThreads = new ArrayList<BTreeInserter>();
    // for(int i = 0; i < 200; i++) {
    //     BTreeInserter bi = startInserter(bf, getRandomTupleData(),
    // insertedTuples);     insertThreads.add(bi);
    //     // The first few inserts will cause pages to split so give them a
    // little     // more time to avoid too many deadlock situations
    //     Thread.sleep(r.nextInt(POLL_INTERVAL));
    // }

    // for(int i = 0; i < 800; i++) {
    //     BTreeInserter bi = startInserter(bf, getRandomTupleData(),
    // insertedTuples);     insertThreads.add(bi);
    // }

    // // wait for all threads to finish
    // waitForInserterThreads(insertThreads);
    // assertTrue(insertedTuples.size() > size);

    // // now insert and delete tuples at the same time
    // System.out.println("Inserting and deleting tuples...");
    // ArrayList<BTreeDeleter> deleteThreads = new ArrayList<BTreeDeleter>();
    // for(BTreeInserter thread : insertThreads) {
    //     thread.rerun(bf, getRandomTupleData(), insertedTuples);
    //     BTreeDeleter bd = startDeleter(bf, insertedTuples);
    //     deleteThreads.add(bd);
    // }

    // // wait for all threads to finish
    // waitForInserterThreads(insertThreads);
    // waitForDeleterThreads(deleteThreads);
    // int numPages = bf.numPages();
    // size = insertedTuples.size();

    // // now delete a bunch of tuples
    // System.out.println("Deleting tuples...");
    // for(int i = 0; i < 10; i++) {
    //     for(BTreeDeleter thread : deleteThreads) {
    //         thread.rerun(bf, insertedTuples);
    //     }

    //     // wait for all threads to finish
    //     waitForDeleterThreads(deleteThreads);
    // }
    // assertTrue(insertedTuples.size() < size);
    // size = insertedTuples.size();

    // // now insert a bunch of random tuples again
    // System.out.println("Inserting tuples...");
    // for(int i = 0; i < 10; i++) {
    //     for(BTreeInserter thread : insertThreads) {
    //         thread.rerun(bf, getRandomTupleData(), insertedTuples);
    //     }

    //     // wait for all threads to finish
    //     waitForInserterThreads(insertThreads);
    // }
    // assertTrue(insertedTuples.size() > size);
    // size = insertedTuples.size();
    // // we should be reusing the deleted pages
    // assertTrue(bf.numPages() < numPages + 20);

    // // kill all the threads
    // insertThreads = null;
    // deleteThreads = null;

    // ArrayList<ArrayList<Integer>> tuplesList = new
    // ArrayList<ArrayList<Integer>>(); tuplesList.addAll(insertedTuples);
    // TransactionId tid = new TransactionId();

    // // First look for random tuples and make sure we can find them
    // System.out.println("Searching for tuples...");
    // for(int i = 0; i < 10000; i++) {
    //     int rand = r.nextInt(insertedTuples.size());
    //     ArrayList<Integer> tuple = tuplesList.get(rand);
    //     IntField randKey = new IntField(tuple.get(bf.keyField()));
    //     IndexPredicate ipred = new IndexPredicate(Op.EQUALS, randKey);
    //     DbFileIterator it = bf.indexIterator(tid, ipred);
    //     it.open();
    //     boolean found = false;
    //     while(it.hasNext()) {
    //         Tuple t = it.next();
    //         if(tuple.equals(SystemTestUtil.tupleToList(t))) {
    //             found = true;
    //             break;
    //         }
    //     }
    //     assertTrue(found);
    //     it.close();
    // }

    // // now make sure all the tuples are in order and we have the right number
    // System.out.println("Performing sanity checks...");
    // DbFileIterator it = bf.iterator(tid);
    // Field prev = null;
    // it.open();
    // int count = 0;
    // while(it.hasNext()) {
    //     Tuple t = it.next();
    //     if(prev != null) {
    //         assertTrue(t.getField(bf.keyField()).compare(Op.
    // GREATER_THAN_OR_EQ, prev));     }
    //     prev = t.getField(bf.keyField());
    //     count++;
    // }
    // it.close();
    // assertEquals(count, tuplesList.size());
    // Database.getBufferPool().transactionComplete(tid);

    // // set the page size back
    // BufferPool.resetPageSize();
}
