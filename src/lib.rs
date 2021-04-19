mod bufferpool;
mod database;
mod field;
mod page;
mod page_id;
mod permissions;
mod row;
mod sequential_scan;
mod table;
mod transaction_id;
mod tuple;
mod util;

mod btree;
mod btree_system_test;
mod btree_unit_test;

mod log;

#[cfg(test)]
mod tests {

    use crate::database::*;

    use crate::table::*;
    use crate::transaction_id::*;

    use log::{debug, info};

    use std::collections::HashMap;
    use std::panic;

    use std::sync::Arc;

    use std::sync::Once;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(init_log);

        Database::global().get_buffer_pool().clear();
    }

    // #[test]
    fn init_log() {
        use env_logger::Builder;

        use std::io::Write;

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

    mod heap_table_test {}

    mod scan_test {
        use super::*;
        use crate::sequential_scan::SequentialScan;
        use std::sync::RwLock;

        #[test]
        // java: simpledb.systemtest.ScanTest#testSmall
        fn test_small() {
            setup();

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

            // clear buffer
            {
                Database::global().get_buffer_pool().clear();
            }

            let mut cells: Vec<Vec<i32>> = Vec::new();
            let table = create_random_heap_table(columns, rows, 10000, HashMap::new(), &mut cells);
            let table_pointer = Arc::new(RwLock::new(table));

            debug!("cells<{} in total>: {:?}", cells.len(), cells);

            Database::add_table(Arc::clone(&table_pointer), "table", "");

            let tabld_id = table_pointer.try_read().unwrap().get_id();

            let scan = SequentialScan::new(TransactionID::new(), tabld_id, "");

            let mut row_index = 0;
            for actual_row in scan {
                // compare cells and rows
                assert!(actual_row.equal_cells(&cells[row_index]));
                row_index += 1;
            }

            info!(
                "scanned: {}, origin dataset length: {}",
                row_index,
                cells.len()
            );
            assert!(row_index == cells.len());
        }

        #[test]
        // Test that rewinding a SeqScan iterator works.
        // simpledb.systemtest.ScanTest#testRewind
        fn test_rewind() {
            setup();

            // create the table
            let mut cells: Vec<Vec<i32>> = Vec::new();
            let rows = 1000;
            let table = create_random_heap_table(2, rows, 10000, HashMap::new(), &mut cells);
            let tabld_id = table.get_id();
            let table_pointer = Arc::new(RwLock::new(table));
            Database::add_table(Arc::clone(&table_pointer), "table", "");

            let mut scan = SequentialScan::new(TransactionID::new(), tabld_id, "");

            // scan the table
            let mut row_index = 0;
            for actual_row in scan.by_ref() {
                assert!(actual_row.equal_cells(&cells[row_index]));
                row_index += 1;
                if row_index >= 100 {
                    break;
                }
            }
            info!("scanned: {}", row_index,);

            // rewind
            scan.rewind();

            // scan the table
            let mut row_index = 0;
            for actual_row in scan.by_ref() {
                assert!(actual_row.equal_cells(&cells[row_index]));
                row_index += 1;
                if row_index >= 100 {
                    break;
                }
            }
        }

        #[test]
        // Verifies that the buffer pool is actually caching data.
        // java: simpledb.systemtest.ScanTest#testCache
        fn test_cache() {
            setup();

            // create the table
            let mut cells: Vec<Vec<i32>> = Vec::new();
            let pages = 30;
            let rows = 992 * pages;
            let table = create_random_heap_table(1, rows, 10000, HashMap::new(), &mut cells);
            debug!("cells: {:?}", cells);
            let table_pointer = Arc::new(RwLock::new(table));
            Database::add_table(Arc::clone(&table_pointer), "table", "");

            let tabld_id = table_pointer.try_read().unwrap().get_id();
            let mut scan = SequentialScan::new(TransactionID::new(), tabld_id, "");

            // scan the table once
            debug!(
                "table read count: {}",
                table_pointer.try_read().unwrap().read_count
            );
            let mut row_index = 0;
            for actual_row in scan.by_ref() {
                debug!(
                    "row index: {}, expect: {:?}, actual: {}",
                    row_index, cells[row_index], actual_row
                );
                if !actual_row.equal_cells(&cells[row_index]) {
                    panic!("row index: {}", row_index);
                }
                row_index += 1;
            }
            info!(
                "scanned: {}, origin dataset length: {}",
                row_index,
                cells.len()
            );
            assert!(row_index == cells.len());
            debug!(
                "table read count: {}",
                table_pointer.try_read().unwrap().read_count
            );

            // rewind
            scan.rewind();
            info!("scan rewind");

            // scan the table again
            row_index = 0;
            for actual_row in scan.by_ref() {
                assert!(actual_row.equal_cells(&cells[row_index]));
                row_index += 1;
            }
            info!(
                "scanned: {}, origin dataset length: {}",
                row_index,
                cells.len()
            );
            assert!(row_index == cells.len());
            debug!(
                "table read count: {}",
                table_pointer.try_read().unwrap().read_count
            );
        }
    }
}
