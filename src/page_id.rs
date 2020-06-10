pub trait PageID {}

pub struct HeapPageID {
    pub table_id: i32,
    pub page_index: i32,
}

impl HeapPageID {
    // pub fn new(table_id)
    // pub fn get_table_id(&self) -> i32 {
    // self.table_id
    //    }
}

impl PageID for HeapPageID {}
