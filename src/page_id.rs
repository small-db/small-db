// pub trait PageID {}
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct HeapPageID {
    pub table_id: i32,
    pub page_index: usize,
}

impl HeapPageID {}
