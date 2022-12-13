use super::{
    BTreeBasePage, BTreePage, BTreePageID, PageCategory,
    EMPTY_PAGE_ID,
};
use crate::{
    btree::{buffer_pool::BufferPool, tuple::TupleScheme},
    io::{SmallReader, SmallWriter, Vaporizable},
};

/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: root page index
/// - 4 bytes: root page category (leaf/internal)
/// - 4 bytes: header page index
pub struct BTreeRootPointerPage {
    base: BTreeBasePage,

    /// The type of this field is `BTreePageID` instead of
    /// `Option<BTreePageID>` because the root page is always
    /// present in the B+ tree.
    ///
    /// This decision also simplified the code.
    root_pid: BTreePageID,

    /// TODO: mandatory the presence of a header page?
    header_page_index: u32,
}

impl BTreeRootPointerPage {
    fn new(pid: &BTreePageID, bytes: Vec<u8>) -> Self {
        let mut reader = SmallReader::new(&bytes);

        // read page category
        let page_category = PageCategory::read_from(&mut reader);
        if page_category != PageCategory::RootPointer {
            panic!("invalid page category: {:?}", page_category);
        }

        // read root page index
        let root_page_index = u32::read_from(&mut reader);

        // read root page category
        let root_page_category = PageCategory::read_from(&mut reader);

        // read header page index
        let header_page_index = u32::read_from(&mut reader);

        let root_pid = BTreePageID {
            category: root_page_category,
            page_index: root_page_index,
            table_id: pid.get_table_id(),
        };
        Self {
            base: BTreeBasePage::new(pid),
            root_pid,
            header_page_index,
        }
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }

    /// Get the id of the first header page
    pub fn get_header_pid(&self) -> Option<BTreePageID> {
        if self.header_page_index == EMPTY_PAGE_ID {
            None
        } else {
            Some(BTreePageID::new(
                PageCategory::Header,
                self.get_pid().table_id,
                self.header_page_index,
            ))
        }
    }

    /// Set the page id of the first header page
    pub fn set_header_pid(&mut self, pid: &BTreePageID) {
        self.header_page_index = pid.page_index;
    }
}

impl BTreePage for BTreeRootPointerPage {
    fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        _tuple_scheme: &TupleScheme,
        _key_field: usize,
    ) -> Self {
        Self::new(pid, bytes)
    }

    fn get_pid(&self) -> BTreePageID {
        self.base.get_pid()
    }

    fn get_parent_pid(&self) -> BTreePageID {
        self.base.get_parent_pid()
    }

    fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.base.set_parent_pid(pid)
    }

    fn get_page_data(&self) -> Vec<u8> {
        let mut writer = SmallWriter::new();

        // write page category
        writer.write(&self.get_pid().category);

        // write root page index
        writer.write(&self.root_pid.page_index);

        // write root page category
        writer.write(&self.root_pid.category);

        // write header page index
        writer.write(&self.header_page_index);

        return writer.to_padded_bytes(BufferPool::get_page_size());
    }
}
