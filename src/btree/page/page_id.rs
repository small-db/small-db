use std::fmt;

use crate::btree::buffer_pool::BufferPool;

pub const EMPTY_PAGE_ID: usize = 0;

#[derive(PartialEq, Copy, Clone, Eq, Hash, Debug)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

// PageID identifies a unique page, and contains the
// necessary metadata
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    // category indicates the category of the page
    pub category: PageCategory,

    // page_index represents the position of the page in
    // the table, start from 0
    pub page_index: usize,

    pub table_id: i32,
}

impl fmt::Display for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}_{}", self.category, self.page_index,)
    }
}

impl fmt::Debug for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl BTreePageID {
    pub fn new(
        category: PageCategory,
        table_id: i32,
        page_index: usize,
    ) -> Self {
        Self {
            category,
            page_index,
            table_id,
        }
    }

    pub fn empty() -> Self {
        Self {
            category: PageCategory::RootPointer,
            page_index: 0,
            table_id: 0,
        }
    }

    pub fn get_table_id(&self) -> i32 {
        self.table_id
    }

    pub fn get_short_repr(&self) -> String {
        format!("{:?}_{}", self.category, self.page_index)
    }
}

pub fn empty_page_data() -> Vec<u8> {
    let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
    data
}
