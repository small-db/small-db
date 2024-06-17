use std::fmt;

use super::PageCategory;
use crate::io::{Serializeable, SmallWriter};

pub const ROOT_PTR_PAGE_ID: u32 = 0;
pub const EMPTY_PAGE_ID: u32 = 9990;

// PageID identifies a unique page, and contains the
// necessary metadata
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BTreePageID {
    /// category indicates the category of the page
    pub(crate) category: PageCategory,

    /// page_index represents the position of the page in
    /// the table, start from 0
    pub(crate) page_index: u32,

    pub(crate) table_id: u32,
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

    pub(crate) fn get_table_id(&self) -> u32 {
        self.table_id
    }

    pub(crate) fn get_root_ptr_page_id(table_id: u32) -> Self {
        BTreePageID {
            category: PageCategory::RootPointer,
            page_index: ROOT_PTR_PAGE_ID,
            table_id,
        }
    }

    pub(crate) fn get_short_repr(&self) -> String {
        format!("{:?}_{}", self.category, self.page_index)
    }

    pub(crate) fn need_page_latch(&self) -> bool {
        if cfg!(feature = "tree_latch") {
            // For the "tree_latch" mode, only leaf pages need a latch.
            return self.category == PageCategory::Leaf;
        } else if cfg!(feature = "page_latch") {
            // For the "page_latch" mode, every page needs a latch.
            return true;
        } else {
            panic!("no latch strategy specified");
        }
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
