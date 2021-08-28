use log::info;
use simple_db_rust::*;
use std::{cell::RefCell, rc::Rc};
mod common;

#[test]
fn insert_tuple() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    let mut insert_value = 0;

    // we should be able to add 502 tuples on one page
    let mut insert_count = 502;
    info!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(tuple);
        insert_value += 1;
        assert_eq!(1, table.pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    insert_count = 251;
    info!("start insert, count: {}", insert_count);
    for _ in 0..insert_count {
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        table.insert_tuple(tuple);
        insert_value += 1;

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    info!("start insert, count: {}", 1);
    let tuple = Tuple::new_btree_tuple(insert_value, 2);
    table.insert_tuple(tuple);

    // there are 4 pages: 1 root page + 3 leaf pages
    assert_eq!(4, table.pages_count());

    // now make sure the records are sorted on the key field
    let it = table.iterator();
    for (i, tuple) in it.enumerate() {
        assert_eq!(i, tuple.get_field(0).value as usize);
    }
}

#[test]
fn insert_duplicate_tuples() {
    common::setup();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    // add a bunch of identical tuples
    let repetition_count = 600;
    for i in 0..5 {
        for _ in 0..repetition_count {
            let tuple = Tuple::new_btree_tuple(i, 2);
            table.insert_tuple(tuple);
        }
    }

    // now search for some ranges and make sure we find all the tuples
    let predicate = Predicate::new(Op::Equals, field::IntField::new(0));
    let it = btree::file::BTreeTableSearchIterator::new(&table, predicate);
    assert_eq!(it.count(), repetition_count);
}
