// use crate::btree::catalog::Catalog;
// use log::{debug, info};

// use crate::btree;

#[test]
fn insert_rows() {
    // use
    // use simple_db_rust::db;
    // use simple_db_rust::btree;
    // use simple_db_rust::tuple;

    // use crate::tuple::Tuple;
    // use crate::{btree::file::BTreeFile, log::init_log, tuple::simple_int_tuple_scheme};
    use std::{cell::RefCell, rc::Rc};

    init_log();

    // create an empty B+ tree file keyed on the second field of a 2-field tuple
    let path = "btree.db";
    let row_scheme = simple_db_rust::test_utils::simple_int_tuple_scheme(2, "");
    let btree_file = Rc::new(RefCell::new(simple_db_rust::BTreeFile::new(
        path, 1, row_scheme,
    )));
    let catalog = simple_db_rust::Catalog::global();
    catalog.add_table(Rc::clone(&btree_file));

    // we should be able to add 502 tuples on one page
    for i in 0..502 {
        let tuple = simple_db_rust::Tuple::new_btree_tuple(i, 2);
        btree_file.borrow().insert_tuple(tuple);
        assert_eq!(1, btree_file.borrow().pages_count());
    }

    // the next 251 tuples should live on page 2 since they are greater than
    // all existing tuples in the file
    for i in 502..(502 + 251) {
        let tuple = simple_db_rust::Tuple::new_btree_tuple(i, 2);
        btree_file.borrow().insert_tuple(tuple);

        // there are 3 pages: 1 root page + 2 leaf pages
        assert_eq!(3, btree_file.borrow().pages_count());
    }
}

// pub fn simple_int_tuple_scheme(width: i32, name_prefix: &str) -> TupleScheme {
//     let mut fields: Vec<FieldItem> = Vec::new();
//     for i in 0..width {
//         let field = FieldItem {
//             field_name: format!("{}-{}", name_prefix, i),
//             field_type: Type::INT,
//         };
//         fields.push(field);
//     }

//     TupleScheme { fields: fields }
// }

use env_logger::Builder;
use std::io::Write;

pub fn init_log() {
    let mut builder = Builder::from_default_env();
    builder
        .format_timestamp_secs()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} - {}] [{}:{}] {}",
                record.level(),
                record.target(),
                record.file().unwrap(),
                record.line().unwrap(),
                record.args()
            )
        })
        .init();
}
