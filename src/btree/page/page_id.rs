use std::fmt;

use super::PageCategory;
use crate::{
    btree::buffer_pool::BufferPool,
    io::{Serializeable, SmallWriter},
};

pub const EMPTY_PAGE_ID: u32 = 0;

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
        write!(
            f,
            "{:?}_{}(table_{})",
            self.category, self.page_index, self.table_id
        )
    }
}

impl fmt::Debug for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl BTreePageID {
    pub fn new(category: PageCategory, table_id: u32, page_index: u32) -> Self {
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

impl Serializeable for BTreePageID {
    type Reference = ();

    fn encode(&self, writer: &mut SmallWriter, _: &Self::Reference) {
        self.category.encode(writer, &());
        self.page_index.encode(writer, &());
        self.table_id.encode(writer, &());
    }

    fn decode<R: std::io::Read>(reader: &mut R, _: &Self::Reference) -> Self {
        let category = PageCategory::decode(reader, &());
        let page_index = u32::decode(reader, &());
        let table_id = u32::decode(reader, &());
        Self {
            category,
            page_index,
            table_id,
        }
    }
}

pub fn empty_page_data() -> Vec<u8> {
    let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
    data
}
