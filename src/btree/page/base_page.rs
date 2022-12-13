use super::{BTreePage, BTreePageID, PageCategory};
use crate::btree::{buffer_pool::BufferPool, tuple::TupleScheme};

const EMPTY_PAGE_TOKEN: [u8; 4] = [55, 55, 55, 55];

pub struct BTreeBasePage {
    pid: BTreePageID,
    parent_page_index: u32,
}

impl BTreeBasePage {
    pub fn new(pid: &BTreePageID) -> BTreeBasePage {
        BTreeBasePage {
            pid: pid.clone(),
            parent_page_index: 0,
        }
    }

    /// Static method to generate a byte array corresponding to an
    /// empty BTreePage.
    ///
    /// Used to add new, empty pages to the file.
    ///
    /// Passing the results of this method to the following
    /// constructors will create a BTreePage with no valid entries
    /// in it.
    /// - `BTreeInternalPage`
    /// - `BTreeLeafPage`
    pub fn empty_page_data() -> Vec<u8> {
        let mut data: Vec<u8> = vec![0; BufferPool::get_page_size()];

        // write the empty page token to the first 4 bytes of the page
        data[0..4].copy_from_slice(&EMPTY_PAGE_TOKEN);

        data
    }

    pub fn is_empty_page(bytes: &[u8]) -> bool {
        bytes[0..4] == EMPTY_PAGE_TOKEN
    }
}

impl BTreePage for BTreeBasePage {
    fn new(
        pid: &BTreePageID,
        _bytes: Vec<u8>,
        _tuple_scheme: &TupleScheme,
        _key_field: usize,
    ) -> Self {
        Self::new(pid)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    fn get_parent_pid(&self) -> BTreePageID {
        let category: PageCategory;
        if self.parent_page_index == 0 {
            category = PageCategory::RootPointer;
        } else {
            category = PageCategory::Internal;
        }
        BTreePageID::new(
            category,
            self.pid.get_table_id(),
            self.parent_page_index,
        )
    }

    fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.parent_page_index = pid.page_index;
    }

    fn get_page_data(&self) -> Vec<u8> {
        unimplemented!()
    }
}
