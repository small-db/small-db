use crate::row::RowScheme;
use crate::cell::*;
use log::{debug, info, error};
use std::collections::HashMap;
use rand::Rng;
use std::fs::File;
use std::io::prelude::*;
use bit_vec::BitVec;
use crate::row::*;
use log::Level::Debug;

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

pub fn create_random_heap_table(
    columns: i32,
    rows: i32,
    max_value: i32,
    column_specification: HashMap<i32, i32>,
    cells: Vec<Vec<i32>>,
) {
//    generate cells
    let mut new_cells: Vec<Vec<i32>> = Vec::new();
    for _ in 0..rows {
        let mut row_cells: Vec<i32> = Vec::new();
        for _ in 0..columns {
            let value = rand::thread_rng().gen_range(1, max_value);
            row_cells.push(value);
        }
        new_cells.push(row_cells);
    }

//    write cells to a readable file
    let mut file = File::create("readable.txt").unwrap();
    for row_cells in &new_cells {
        for value in row_cells {
            file.write_fmt(format_args!("{} ", value));
        }
        file.write(b"\n");
    }

//    write cells to a heap file
    let bytes_per_page: usize = 1024;
    let mut bytes_per_row: usize = 0;
    let row_scheme: RowScheme = simple_int_row_scheme(columns, "");
    for i in 0..columns {
        bytes_per_row += get_type_length(row_scheme.get_field_type(i));
    }
    debug!("bytes per row: {}", bytes_per_row);
    let mut rows_per_page = (bytes_per_page * 8) / (bytes_per_row * 8 + 1);
    debug!("rows per page: {}", rows_per_page);
    let mut header_bytes = rows_per_page / 8;
//    ceiling
    if header_bytes * 8 < rows_per_page {
        header_bytes += 1;
    }
    debug!("header size: {} bytes", header_bytes);

//    pagination
    let mut paginated_cells: Vec<Vec<Vec<i32>>> = Vec::new();

    let mut start: usize = 0;
    let mut end: usize = start + rows_per_page as usize;
    while start <= rows as usize {
        if end + 1 > rows as usize {
            end = rows as usize;
        }

        debug!("sub cells from {} to {}", start, end);
        let sub_cells = &new_cells[start..end];
        debug!("sub cells length: {}", sub_cells.len());
        paginated_cells.push(sub_cells.to_vec());

        start += rows_per_page as usize;
        end = start + rows_per_page as usize;
    }

    let mut file = File::create("heap.db").unwrap();
    for sub_cells in &paginated_cells {
//    constract header
        let mut bv = BitVec::from_elem(header_bytes as usize * 8, false);
        for i in 0..sub_cells.len() {
            bv.set(i, true);
        }
        debug!("bit vec: {:?}", bv);

//    write header
        file.write(&bv.to_bytes());

//        write data
        for row in sub_cells {
            for cell in row {
                file.write(&cell.to_be_bytes());
            }
        }

//        padding
        let padding_bytes: usize = bytes_per_page - bv.to_bytes().len() - bytes_per_row * sub_cells.len();
        debug!("padding size: {} bytes", padding_bytes);
//        TODO: update slice init
        let bytes_array = [0 as u8; 4096];
        file.write(&bytes_array[0..padding_bytes]);
    }
}
