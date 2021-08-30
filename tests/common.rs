use rand::prelude::*;
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

use simple_db_rust::*;

pub fn setup() {
    test_utils::init_log();
    btree::buffer_pool::BufferPool::global().clear();
}

pub fn create_random_btree_table(
    columns: i32,
    rows: i32,
) -> Rc<RefCell<BTreeTable>> {
    let path = "btree.db";
    let row_scheme = test_utils::simple_int_tuple_scheme(columns, "");
    let table_ref = Rc::new(RefCell::new(BTreeTable::new(path, 1, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));

    {
        // The borrow lasts until the returned Ref exits scope.
        let table = table_ref.borrow();
        let mut rng = rand::thread_rng();
        for _ in 0..rows {
            let insert_value = rng.gen_range(0, i32::MAX);
            let tuple = Tuple::new_btree_tuple(insert_value, 2);
            table.insert_tuple(tuple);
        }
    } // The borrow to table_ref is released here.

    return table_ref;
}
