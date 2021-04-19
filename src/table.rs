use crate::database::*;
use crate::field::*;
use crate::page::*;
use crate::row::RowScheme;
use crate::row::*;
use bit_vec::BitVec;
use log::debug;
use rand::Rng;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use io::ErrorKind;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub struct HeapTable {
    pub table_id: i32,
    pub file: Arc<Mutex<File>>,
    pub row_scheme: Arc<RowScheme>,
    pub read_count: i32,
}

impl HeapTable {
    pub fn new(file_path: &str, row_scheme: RowScheme) -> HeapTable {
        let file = File::open(file_path).unwrap();
        HeapTable {
            table_id: 0,
            file: Arc::new(Mutex::new(file)),
            row_scheme: Arc::new(row_scheme),
            read_count: 0,
        }
    }

    pub fn get_row_scheme(&self) -> Arc<RowScheme> {
        Arc::clone(&self.row_scheme)
    }

    pub fn get_id(&self) -> i32 {
        self.table_id
    }

    pub fn get_num_pages(&self) -> usize {
        let metadata = self.file.try_lock().unwrap().metadata().unwrap();
        let n = metadata.len() as f64 / Database::global().get_buffer_pool().get_page_size() as f64;
        // round::cell(n, 0) as usize
        n.ceil() as usize
    }

    pub fn get_file(&self) -> MutexGuard<File> {
        match self.file.try_lock() {
            Ok(a) => a,
            _ => unreachable!(),
        }
    }

    pub fn read_page(&mut self, page_id: usize) -> Result<HeapPage, io::Error> {
        debug!("read page, table: {}, page: {}", self.table_id, page_id);

        self.read_count += 1;

        let file_size = self.get_file().metadata().unwrap().len();
        let seek_pos = page_id as u64 * 4096;
        if seek_pos >= file_size {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!(
                    "seek failed, file size: {}, seek position: {}",
                    file_size, seek_pos
                ),
            ));
        }

        match self.get_file().seek(SeekFrom::Start(seek_pos)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let mut buf: [u8; 4096] = [0; 4096];
        self.get_file().read_exact(&mut buf);
        debug!("read page {} from file {:?}", page_id, self.get_file());

        let mut bytes: Vec<u8> = Vec::new();
        for b in buf.iter() {
            bytes.push(*b);
        }

        let page = HeapPage::new(self.get_row_scheme(), bytes);
        Ok(page)
    }
}

pub fn create_random_heap_table(
    columns: i32,
    rows: i32,
    max_value: i32,
    _column_specification: HashMap<i32, i32>,
    new_cells: &mut Vec<Vec<i32>>,
) -> HeapTable {
    debug!("rows count: {}", rows);

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
    let path = "./readable.txt";
    let mut file = File::create(path).unwrap();
    for row_cells in new_cells.iter() {
        for value in row_cells {
            file.write_fmt(format_args!("{} ", value));
        }
        file.write(b"\n");
    }

    // write cells to a heap file
    let mut bytes_per_row: usize = 0;
    let row_scheme: RowScheme = simple_int_row_scheme(columns, "");
    for i in 0..columns {
        bytes_per_row += get_type_length(row_scheme.get_field_type(i));
        debug!("bytes per row: {}", bytes_per_row);
    }
    debug!("bytes per row: {}", bytes_per_row);
    let rows_per_page = (4096 * 8) / (bytes_per_row * 8 + 1);
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
    while start < rows as usize {
        if end + 1 > rows as usize {
            end = rows as usize;
        }

        debug!("sub cells (rows) from {} to {}", start, end);
        let sub_cells = &new_cells[start..end];
        debug!("sub cells (rows) length: {}", sub_cells.len());
        paginated_cells.push(sub_cells.to_vec());

        start += rows_per_page as usize;
        end = start + rows_per_page as usize;
    }

    let table_path = "./heap.db";
    let mut file = File::create(table_path).unwrap();
    let mut pages: usize = 0;
    for sub_cells in &paginated_cells {
        // constract header
        let mut bv = BitVec::from_elem(header_bytes as usize * 8, false);
        for i in 0..sub_cells.len() {
            bv.set(i, true);
        }

        // write header
        let header = bv.to_bytes();
        file.write(&header);

        // write data
        for row in sub_cells {
            for cell in row {
                file.write(&cell.to_be_bytes());
            }
        }

        // padding
        let padding_bytes: usize = 4096 - bv.to_bytes().len() - bytes_per_row * sub_cells.len();
        debug!("padding size: {} bytes", padding_bytes);
        let bytes_array = [0 as u8; 4096];
        file.write(&bytes_array[0..padding_bytes]);
        pages += 1;

        let file_size = file.metadata().unwrap().len();
        debug!("page size: {}, file size: {}", pages, file_size);
        assert!(pages * 4096 == file_size as usize);
    }

    // write a page for empty table (prevent scan init from read failure)
    if paginated_cells.len() == 0 {
        let bytes_array = [0 as u8; 4096];
        debug!("padding size: {} bytes", 4096);
        file.write(&bytes_array);
        pages += 1;
    }

    let row_scheme = simple_int_row_scheme(columns, "");
    let table = HeapTable::new(table_path, row_scheme);

    let file_size = file.metadata().unwrap().len();
    debug!("page size: {}, file size: {}", pages, file_size);
    assert!(pages * 4096 == file_size as usize);

    table
}
