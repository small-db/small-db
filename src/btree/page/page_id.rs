use std::fmt;

use crate::btree::buffer_pool::BufferPool;

pub const EMPTY_PAGE_ID: u32 = 0;

#[derive(PartialEq, Copy, Clone, Eq, Hash, Debug)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

impl PageCategory {
    /// serialize to 4 bytes
    pub fn to_bytes(&self) -> [u8; 4] {
        match self {
            PageCategory::RootPointer => [0, 0, 0, 0],
            PageCategory::Internal => [0, 0, 0, 1],
            PageCategory::Leaf => [0, 0, 0, 2],
            PageCategory::Header => [0, 0, 0, 3],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes {
            [0, 0, 0, 0] => PageCategory::RootPointer,
            [0, 0, 0, 1] => PageCategory::Internal,
            [0, 0, 0, 2] => PageCategory::Leaf,
            [0, 0, 0, 3] => PageCategory::Header,
            _ => panic!("invalid page category: {:?}", bytes),
        }
    }
}

// PageID identifies a unique page, and contains the
// necessary metadata
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    /// category indicates the category of the page
    pub category: PageCategory,

    /// page_index represents the position of the page in
    /// the table, start from 0
    pub page_index: u32,

    pub table_id: u32,
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
        table_id: u32,
        page_index: u32,
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

    pub fn get_table_id(&self) -> u32 {
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
