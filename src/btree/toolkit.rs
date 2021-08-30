use std::{cell::RefCell, rc::Rc};

use rand::Rng;

use crate::{BTreeTable, Catalog, Tuple, util::{self, simple_int_tuple_scheme}};

// A toolkit used for tests.

/*
Create a table with a given number of rows and columns.

The rows are filled with random data and are sorted by the
key field/column before being inserted into the table.
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
        for t in tuples {
            table.insert_tuple(t);
        }
    } // The borrow to table_ref is released here.

    return table_ref;
}