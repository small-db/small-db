use std::path::Path;

use crate::{btree::file::BTreeFile, tuple::simple_int_tuple_scheme};
use crate::btree::tuple::BTreeTuple;
use crate::tuple::Tuple;


#[test]
fn insert_rows() {
    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = Path::new("btree.db");
    let row_scheme = simple_int_tuple_scheme(2, "");
    let tree = BTreeFile::new(path, 1, row_scheme);

    // we should be able to add 502 tuples on one page
    for i in 0..502 {
        let tuple = Tuple::new_btree_tuple(i, 2);
        tree.insert_tuple(tuple);
        assert_eq!(1, tree.num_pages());
    }
}