use rand::prelude::*;
use std::{cell::RefCell, rc::Rc};

use simple_db_rust::{
    btree::table::SplitStrategy, util::simple_int_tuple_scheme, *,
};

pub fn setup() {
    test_utils::init_log();
    btree::buffer_pool::BufferPool::global().clear();
}

/**
Create a table with a given number of rows and columns.

The rows are filled with random data and are sorted by the
key field/column before being inserted into the table.

The rows are inserted to pages in a compact manner. Result
in all leaf pages being full.
*/
pub fn create_random_btree_table(
    columns: i32,
    rows: i32,
) -> Rc<RefCell<BTreeTable>> {
    let path = "btree.db";
    let key_field = 0;
    let row_scheme = simple_int_tuple_scheme(columns, "");
    let table_ref =
        Rc::new(RefCell::new(BTreeTable::new(path, key_field, row_scheme)));
    Catalog::global().add_table(Rc::clone(&table_ref));

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..rows {
        let insert_value = rng.gen_range(0, i32::MAX);
        let tuple = Tuple::new_btree_tuple(insert_value, 2);
        tuples.push(tuple);
    }

    tuples.sort_by(|a, b| a.get_field(key_field).cmp(&b.get_field(key_field)));

    {
        // The borrow lasts until the returned Ref exits scope.
        let table = table_ref.borrow();

        // Using specific strategy to fill every page.
        table.set_split_strategy(SplitStrategy::AddRightWithoutMove);
        for t in tuples {
            table.insert_tuple(t);
        }
        // Revert to default split strategy.
        table.set_split_strategy(SplitStrategy::MoveHalfToRight)
    } // The borrow to table_ref is released here.

    return table_ref;
}
