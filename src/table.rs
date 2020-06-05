use crate::row::RowScheme;
use crate::cell::*;
use log::{debug, info, error};
use std::collections::HashMap;
use rand::Rng;
use std::fs::File;
use std::io::prelude::*;

pub trait Table {
    fn get_row_scheme(&self) -> &RowScheme;
    fn get_id(&self) -> i32;
}

pub struct SkeletonTable {
    pub table_id: i32,
    pub row_scheme: RowScheme,
}

impl Table for SkeletonTable {
    fn get_row_scheme(&self) -> &RowScheme {
        &self.row_scheme
    }

    fn get_id(&self) -> i32 {
        self.table_id
    }
}

pub struct HeapTable {
    pub table_id: i32,
    pub row_scheme: RowScheme,
}

impl Table for HeapTable {
    fn get_row_scheme(&self) -> &RowScheme {
        &self.row_scheme
    }

    fn get_id(&self) -> i32 {
        self.table_id
    }
}

//public static File createRandomHeapFileUnopened(int columns, int rows,
//int maxValue, Map<Integer, Integer> columnSpecification,
//ArrayList<ArrayList<Integer>> tuples) throws IOException {
pub fn create_random_heap_table(
    columns: i32,
    rows: i32,
    max_value: i32,
    column_specification: HashMap<i32, i32>,
    tuples: Vec<Vec<i32>>,
) {
//    generate tuples
    let mut new_tuples: Vec<Vec<i32>> = Vec::new();
    for _ in 0..rows {
        let mut row_tuples: Vec<i32> = Vec::new();
        for _ in 0..columns {
            let value = rand::thread_rng().gen_range(1, max_value);
            row_tuples.push(value);
        }
        new_tuples.push(row_tuples);
    }

//    write tuples to a readable file
    let mut file = File::create("readable.txt").unwrap();
    for row_tuples in new_tuples {
        for value in row_tuples {
            file.write_fmt(format_args!("{} ", value));
        }
        file.write(b"\n");
    }

//    write tuples to a heap file
    let bytesPerPage = 1024;
    let mut bytesPerRow = 0;
    use crate::row::*;
    let row_scheme: RowScheme = simple_int_row_scheme(columns, "");
    for i in 0..columns {
        bytesPerRow += get_type_length(row_scheme.get_field_type(i));
    }
    debug!("bytes per row: {}", bytesPerRow);
}
