#[test]
fn insert_rows() {
    use log::info;
    use simple_db_rust::{test_utils, BTreeTable, Catalog, Tuple};
    use std::{cell::RefCell, rc::Rc};

    test_utils::init_log();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(2, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));
    let table = table_ref.borrow();

    // we should be able to add 502 tuples on one page
    info!("start insert, count: {}", 502);
    for i in 0..502 {
        let tuple = Tuple::new_btree_tuple(i, 2);
        table.insert_tuple(tuple);
        assert_eq!(1, table.pages_count());
    }

    let it = table.iterator();
    for (i, tuple) in it.enumerate() {
        info!("i: {}, tuple: {}", i, tuple);
        assert_eq!(i, tuple.get_field(0).value as usize);
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    info!("start insert, count: {}", 251);
    for i in 502..(502 + 251) {
        let tuple = simple_db_rust::Tuple::new_btree_tuple(i, 2);
        table.insert_tuple(tuple);

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, table.pages_count());
    }

    // one more insert greater than 502 should cause page 2 to split
    info!("start insert, count: {}", 1);
    let tuple = simple_db_rust::Tuple::new_btree_tuple(753, 2);
    table.insert_tuple(tuple);
    assert_eq!(4, table.pages_count());

    let it = table.iterator();
    for (i, tuple) in it.enumerate() {
        info!("i: {}, tuple: {}", i, tuple);
        assert_eq!(i, tuple.get_field(0).value as usize);
    }
}
