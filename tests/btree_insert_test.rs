#[test]
fn insert_rows() {
    use log::{debug, info};
    use simple_db_rust::{test_utils, BTreeTable, Catalog, Tuple};
    use std::{cell::RefCell, rc::Rc};

    test_utils::init_log();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let btree_file = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    let catalog = Catalog::global();
    catalog.add_table(Rc::clone(&btree_file));

    // we should be able to add 502 tuples on one page
    info!("start insert, count: {}", 502);
    for i in 0..502 {
        let tuple = Tuple::new_btree_tuple(i, 2);
        btree_file.borrow().insert_tuple(tuple);
        assert_eq!(1, btree_file.borrow().pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    info!("start insert, count: {}", 251);
    for i in 502..(502 + 251) {
        let tuple = simple_db_rust::Tuple::new_btree_tuple(i, 2);
        btree_file.borrow().insert_tuple(tuple);

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, btree_file.borrow().pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    info!("start insert, count: {}", 1);
    let tuple = simple_db_rust::Tuple::new_btree_tuple(753, 2);
    btree_file.borrow().insert_tuple(tuple);
    assert_eq!(4, btree_file.borrow().pages_count());
}
