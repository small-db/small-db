#[test]
fn insert_rows() {
    use std::{cell::RefCell, rc::Rc};

    use crate::btree::database_singleton::singleton_db;
    
    use crate::tuple::Tuple;
    use crate::{btree::file::BTreeFile, log::init_log, tuple::simple_int_tuple_scheme};

    init_log();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = simple_int_tuple_scheme(2, "");
    let btree_file = Rc::new(RefCell::new(BTreeFile::new(path, 1, row_scheme)));
    let catalog = singleton_db().get_catalog();
    catalog.borrow_mut().add_table(Rc::clone(&btree_file));

    // we should be able to add 502 tuples on one page
    for i in 0..502 {
        let tuple = Tuple::new_btree_tuple(i, 2);
        btree_file.borrow().insert_tuple(tuple);
        assert_eq!(1, btree_file.borrow().pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    for i in 502..753 {
        let tuple = Tuple::new_btree_tuple(i, 2);
        btree_file.borrow_mut().insert_tuple(tuple);

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, btree_file.borrow().pages_count());
    }
}
