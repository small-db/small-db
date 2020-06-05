use std::collections::HashMap;
use env_logger;

mod row;
mod cell;
mod table;
mod database;

#[cfg(test)]
mod tests {
    use crate::row::*;
    use crate::cell::*;
    use crate::table::*;
    use crate::database::*;

    use std::panic;

    fn run_test<T>(test: T) -> ()
        where T: FnOnce() -> () + panic::UnwindSafe
    {
//        setup
        env_logger::init();

        let result = panic::catch_unwind(|| {
            test()
        });

        assert!(result.is_ok())
    }

    #[test]
    fn combine() {
        let scheme1 = simple_int_row_scheme(1, "scheme1");
        let scheme2 = simple_int_row_scheme(2, "scheme1");

        let scheme3 = RowScheme::merge(scheme1, scheme2);

        assert_eq!(scheme3.filedsCount(), 3);
    }

    #[test]
    fn get_field_type() {
        let lengths = vec![1, 2, 1000];

        for l in lengths {
            let scheme = simple_int_row_scheme(l, "");
            for i in 0..l {
                assert_eq!(Type::INT, scheme.get_field_type(i));
            }
        }
    }

    #[test]
    fn modify_fields() {
        let scheme = simple_int_row_scheme(2, "");

        let mut row = Row::new(scheme);
        row.set_cell(0, Box::new(IntCell::new(-1)));
        row.set_cell(1, Box::new(IntCell::new(0)));

        assert_eq!(
            IntCell::new(-1),
            *row.get_cell(0).as_any().downcast_ref::<IntCell>().unwrap()
        );
        assert_eq!(
            IntCell::new(0),
            *row.get_cell(1).as_any().downcast_ref::<IntCell>().unwrap()
        );
    }

    #[test]
    fn get_row_scheme() {
        // setup
        let mut db = Database::new();
        let table_id_1 = 3;
        let table_id_2 = 5;
        let table_1 = SkeletonTable {
            table_id: table_id_1,
            row_scheme: simple_int_row_scheme(2, ""),
        };
        let table_2 = SkeletonTable {
            table_id: table_id_2,
            row_scheme: simple_int_row_scheme(2, ""),
        };
        db.get_catalog().add_table(Box::new(table_1), "table1", "");
        db.get_catalog().add_table(Box::new(table_2), "table2", "");

        let expected = simple_int_row_scheme(2, "");
        let actual = db.get_catalog().get_row_scheme(table_id_1);
        assert_eq!(expected, *actual);
    }

    mod heap_table_test {
        use super::*;
        use std::collections::HashMap;

        #[test]
        fn get_id() {
            run_test(|| {

                // setup
                let hf = create_random_heap_table(
                    2,
                    20,
                    1000,
                    HashMap::new(),
                    Vec::new(),
                );
            })
        }
    }
}
