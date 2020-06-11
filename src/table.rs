use crate::bufferpool::*;
use crate::cell::*;
use crate::database::*;
use crate::row::RowScheme;
use crate::row::*;
use bit_vec::BitVec;
// use log::Level::Debug;
use log::{debug, error, info};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use crate::page::{Page, HeapPage};
use std::io::SeekFrom;
use std::rc::Rc;
use std::borrow::BorrowMut;

//pub trait Table: Debug + Send + Sync {
//    fn get_row_scheme(&self) -> Arc<RowScheme>;
//    fn get_id(&self) -> i32;
//    fn get_num_pages(&self) -> usize;
//    fn get_file(&self) -> &File;
//    fn read_page(&self, page_id: i32) -> &Page;
//}

//#[derive(Debug)]
//pub struct SkeletonTable {
//    pub table_id: i32,
//    pub file: File,
//    pub row_scheme: Arc<RowScheme>,
//}
//
//// impl SkeletonTable {
//// pub fn new() -> SkeletonTable {
////
//// }
//// }
//
//impl Table for SkeletonTable {
//    fn get_row_scheme(&self) -> Arc<RowScheme> {
//        // &self.row_scheme
//        Arc::clone(&self.row_scheme)
//    }
//
//    fn get_id(&self) -> i32 {
//        self.table_id
//    }
//
//    fn get_num_pages(&self) -> usize {
//        0
//    }
//
//    fn get_file(&self) -> &File {
//        &self.file
//    }
//}

#[derive(Debug)]
pub struct HeapTable {
    pub table_id: i32,
    pub file: Arc<File>,
    pub row_scheme: Arc<RowScheme>,
}

impl HeapTable {
    pub fn new(file: File, row_scheme: RowScheme) -> HeapTable {
        HeapTable {
            table_id: 0,
            file: Arc::new(file),
            row_scheme: Arc::new(row_scheme),
        }
    }
//}
//
//impl Table for HeapTable {
    // fn get_row_scheme(&self) -> &RowScheme {
    // &self.row_scheme
    // }

    pub fn get_row_scheme(&self) -> Arc<RowScheme> {
        Arc::clone(&self.row_scheme)
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    pub fn get_num_pages(&self) -> usize {
        let metadata = self.file.metadata().unwrap();
        let n = metadata.len() as f64 / BufferPool::get_page_size() as f64;
        // round::cell(n, 0) as usize
        n.ceil() as usize
    }

    pub fn get_file(&self) -> &File {
        &self.file
    }

    pub fn read_page(&mut self, page_id: i32) -> &dyn Page {
//        self.file.seek();
//        self.file .seek(SeekFrom::Start(page_id as u64 * 4096)).unwrap();
//        self.file.
        &HeapPage::new(&[])
    }
}

pub fn create_random_heap_table(
    columns: i32,
    rows: i32,
    max_value: i32,
    column_specification: HashMap<i32, i32>,
    new_cells: &mut Vec<Vec<i32>>,
    // ) -> Box<HeapTable> {
) -> HeapTable {
    // generate cells
    // let mut new_cells: Vec<Vec<i32>> = Vec::new();
    for _ in 0..rows {
        let mut row_cells: Vec<i32> = Vec::new();
        for _ in 0..columns {
            let value = rand::thread_rng().gen_range(1, max_value);
            row_cells.push(value);
        }
        new_cells.push(row_cells);
    }

    // write cells to a readable file
    let mut file = File::create("readable.txt").unwrap();
    for row_cells in new_cells.iter() {
        for value in row_cells {
            file.write_fmt(format_args!("{} ", value));
        }
        file.write(b"\n");
    }

    // write cells to a heap file
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
    // ceiling
    if header_bytes * 8 < rows_per_page {
        header_bytes += 1;
    }
    debug!("header size: {} bytes", header_bytes);

    // pagination
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
        // constract header
        let mut bv = BitVec::from_elem(header_bytes as usize * 8, false);
        for i in 0..sub_cells.len() {
            bv.set(i, true);
        }
        debug!("bit vec: {:?}", bv);

        // write header
        file.write(&bv.to_bytes());

        // write data
        for row in sub_cells {
            for cell in row {
                file.write(&cell.to_be_bytes());
            }
        }

        // padding
        let padding_bytes: usize =
            bytes_per_page - bv.to_bytes().len() - bytes_per_row * sub_cells.len();
        debug!("padding size: {} bytes", padding_bytes);
        let bytes_array = [0 as u8; 4096];
        file.write(&bytes_array[0..padding_bytes]);
    }

    let row_scheme = simple_int_row_scheme(columns, "");
    let table = HeapTable::new(file, row_scheme);

    // let poing = Arc::new(table)
    // add to catalog
    // db.get_catalog().add_table(Arc::new(table), "table", "");

    table
    // Box::new(table)
}
