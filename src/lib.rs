use crate::database::Database;
use env_logger;
use std::collections::HashMap;

mod bufferpool;
mod cell;
mod database;
mod page;
mod page_id;
mod permissions;
mod row;
mod sequential_scan;
mod table;
mod transaction_id;

#[cfg(test)]
mod tests {
    use crate::cell::*;
    use crate::database::*;
    use crate::row::*;
    use crate::table::*;
    use crate::transaction_id::*;

    use log::{debug, error, info};
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::panic;
    use std::rc::Rc;
    use std::sync::Arc;

    // fn run_test<T>(test: T) -> ()
    // where
    // T: FnOnce() -> () + panic::UnwindSafe,
    // {
    // // setup
    // env_logger::init();
    //
    // let result = panic::catch_unwind(|| test());
    //
    // assert!(result.is_ok())
    // }

    // #[test]
    fn init_log() {
        use env_logger::Builder;
        use log::LevelFilter;
        use std::env;
        use std::io::Write;

        let mut builder = Builder::from_default_env();

        builder
            .format_timestamp_secs()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[{} - {}] [{}:{}] {}",
                    // record
                    // builder.format_timestamp_secs(),
                    record.level(),
                    record.target(),
                    record.file().unwrap(),
                    record.line().unwrap(),
                    record.args()
                )
            })
            // .filter(None, LevelFilter::Debug)
            // .format_timestamp_secs()
            .init();
    }

    // #[test]
    // fn combine() {
    // let scheme1 = simple_int_row_scheme(1, "scheme1");
    // let scheme2 = simple_int_row_scheme(2, "scheme1");
    //
    // let scheme3 = RowScheme::merge(scheme1, scheme2);
    //
    // assert_eq!(scheme3.filedsCount(), 3);
    // }
    //
    // #[test]
    // fn get_field_type() {
    // let lengths = vec![1, 2, 1000];
    //
    // for l in lengths {
    // let scheme = simple_int_row_scheme(l, "");
    // for i in 0..l {
    // assert_eq!(Type::INT, scheme.get_field_type(i));
    // }
    // }
    // }

    // #[test]
    // fn modify_fields() {
    // let scheme = simple_int_row_scheme(2, "");
    //
    // let mut row = Row::new(scheme);
    // row.set_cell(0, Box::new(IntCell::new(-1)));
    // row.set_cell(1, Box::new(IntCell::new(0)));
    //
    // assert_eq!(
    // IntCell::new(-1),
    // *row.get_cell(0).as_any().downcast_ref::<IntCell>().unwrap()
    // );
    // assert_eq!(
    // IntCell::new(0),
    // *row.get_cell(1).as_any().downcast_ref::<IntCell>().unwrap()
    // );
    // }

    // #[test]
    // fn get_row_scheme() {
    // // setup
    // // let mut db = Database::new();
    // let table_id_1 = 3;
    // let table_id_2 = 5;
    // let table_1 = SkeletonTable {
    // table_id: table_id_1,
    // row_scheme: Arc::new(simple_int_row_scheme(2, "")),
    // };
    // let table_2 = SkeletonTable {
    // table_id: table_id_2,
    // row_scheme: Arc::new(simple_int_row_scheme(2, "")),
    // };
    // db.get_catalog().add_table(Arc::new(table_1), "table1", "");
    // db.get_catalog().add_table(Arc::new(table_2), "table2", "");
    //
    // let expected = simple_int_row_scheme(2, "");
    // let actual = db.get_catalog().get_row_scheme(table_id_1);
    // assert_eq!(expected, *actual);
    // }

    mod heap_table_test {
        use super::*;

        // struct GlobalVars {
        // db: Database,
        // heap_table: Rc<HeapTable>,
        // row_scheme: RowScheme,
        // }

        // fn set_up() -> GlobalVars {
        // // create db
        // let mut db = Database::new();
        //
        // // create table
        // let table = create_random_heap_table(2, 20, 1000, HashMap::new(), Vec::new());
        // let a: Rc<HeapTable> = Rc::new(table);
        // db.get_catalog().add_table(Rc::clone(&a), "heap table", "");
        //
        // GlobalVars {
        // db: db,
        // heap_table: Rc::clone(&a),
        // row_scheme: simple_int_row_scheme(2, ""),
        // }
        // }
        //
        // #[test]
        // fn get_id() {
        // run_test(|| {
        // // setup
        // let gv = set_up();
        // let mut db = gv.db;
        // let mut heap_table = gv.heap_table;
        //
        // let table_id = Rc::clone(&heap_table).get_id();
        // })
        // }
        //
        // #[test]
        // fn get_row_scheme() {
        // // setup
        // let gv = set_up();
        // let mut db = gv.db;
        // let mut row_scheme = gv.row_scheme;
        // let mut heap_table = gv.heap_table;
        //
        // assert_eq!(row_scheme, *heap_table.get_row_scheme());
        // }
        //
        // #[test]
        // fn get_num_pages() {
        // // setup
        // let gv = set_up();
        // let mut db = gv.db;
        // let mut row_scheme = gv.row_scheme;
        // let mut heap_table = gv.heap_table;
        //
        // debug!("num of pages: {}", heap_table.get_num_pages());
        // assert_eq!(1, heap_table.get_num_pages());
        // }
    }

    mod scan_test {
        use super::*;
        use crate::sequential_scan::SequentialScan;
        use std::sync::{Mutex, RwLock};

        #[test]
        // java: simpledb.systemtest.ScanTest#testSmall
        fn test_small() {
            init_log();
            // let db = Database::new();

            // run_test(|| {
            let column_sizes = [1, 2, 3, 4, 5];
            let row_sizes = [0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + 1000];

            for column_size in &column_sizes {
                for row_size in &row_sizes {
                    validate_sacn(*column_size, *row_size);
                }
            }
        }

        fn validate_sacn(columns: i32, rows: i32) {
            info!("start validate scan, columns: {}, rows: {}", columns, rows);
            let mut cells: Vec<Vec<i32>> = Vec::new();
            let table = create_random_heap_table(columns, rows, 10000, HashMap::new(), &mut cells);
            let table_pointer = Arc::new(RwLock::new(table));

            debug!("cells<{} in total>: {:?}", cells.len(), cells);

            // add table to catolog
            // add a scope to release write lock
            {
                let mut catlog = Database::global().get_write_catalog();
                catlog.add_table(Arc::clone(&table_pointer), "table", "");
            }

            // test if match
            // let tid = TransactionID::new();
            // debug!("tid: {}", tid.id);

            let tabld_id = table_pointer.try_read().unwrap().get_id();

            let mut scan = SequentialScan::new(TransactionID::new(), tabld_id, "");

            let mut row_index = 0;
            for actual_row in scan {
                debug!("{}", actual_row);

                // compare cells and rows
                assert!(actual_row.equal_cells(&cells[row_index]));
                row_index += 1;
            }

            if row_index < cells.len() {
                info!(
                    "scanned rows not enough, scanned: {}, origin: {}",
                    row_index,
                    cells.len()
                );
            }
        }

        #[test]
        // Verifies that the buffer pool is actually caching data.
        // java: simpledb.systemtest.ScanTest#testCache
        fn test_cache() {
            init_log();

            // create the table
            let mut cells: Vec<Vec<i32>> = Vec::new();
            let pages = 30;
            let table = create_random_heap_table(1, 992 * pages, 10000, HashMap::new(), &mut cells);

            // scan the table once

            // remove table file

            // scan the table again

        }
    }
}
