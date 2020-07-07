// pub trait PageID {}
#[derive(PartialEq, Eq, Hash)]
pub struct HeapPageID {
    pub table_id: i32,
    pub page_index: usize,
}

impl HeapPageID {
    // pub fn new(table_id)
    // pub fn get_table_id(&self) -> i32 {
    // self.table_id
    // }
}

// impl PageID for HeapPageID {}
