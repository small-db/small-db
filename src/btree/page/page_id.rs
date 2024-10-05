use std::fmt;

use super::PageCategory;
use crate::io::{Serializeable, SmallWriter};

pub type PageIndex = u32;
pub type TableIndex = u32;

pub(crate) const ROOT_PTR_PAGE_ID: u32 = 0;
pub(crate) const FIRST_HEADER_PID: u32 = 1;
pub(crate) const FIRST_LEAF_PID: u32 = 2;
pub(crate) const EMPTY_PID: u32 = u32::MAX;

// PageID identifies a unique page, and contains the
// necessary metadata
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    /// category indicates the category of the page
    pub(crate) category: PageCategory,

    /// page_index represents the position of the page in
    /// the table, start from 0
    pub(crate) page_index: PageIndex,

    pub(crate) table_id: TableIndex,
}

impl fmt::Display for BTreePageID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}_{}", self.category, self.page_index)
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

    pub(crate) fn get_table_id(&self) -> u32 {
        self.table_id
    }

    pub(crate) fn get_root_ptr_pid(table_id: u32) -> Self {
        BTreePageID {
            category: PageCategory::RootPointer,
            page_index: ROOT_PTR_PAGE_ID,
            table_id,
        }
    }

    /// Return the pid of the first header page of the table.
    pub(crate) fn get_header_pid(table_id: u32) -> Self {
        BTreePageID {
            category: PageCategory::Header,
            page_index: FIRST_HEADER_PID,
            table_id,
        }
    }

    pub(crate) fn need_page_latch(&self) -> bool {
        return true;
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
