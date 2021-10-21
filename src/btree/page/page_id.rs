use std::{convert::TryInto, fmt};

use crate::btree::buffer_pool::BufferPool;

pub const EMPTY_PAGE_ID: usize = 0;

#[derive(PartialEq, Copy, Clone, Eq, Hash)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

impl fmt::Display for PageCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageCategory::RootPointer => {
                write!(f, "ROOT_POINTER")
            }
            PageCategory::Internal => {
                write!(f, "INTERNAL")
            }
            PageCategory::Leaf => {
                write!(f, "LEAF")
            }
            PageCategory::Header => {
                write!(f, "HEADER")
            }
        }
    }
}

impl fmt::Debug for PageCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[test]
fn test_page_category() {
    assert_ne!(PageCategory::Header, PageCategory::Leaf);
    if PageCategory::Leaf == PageCategory::RootPointer {
        println!("error")
    } else {
        println!("ok")
    }
    let c = PageCategory::Header;
    match c {
        PageCategory::Leaf => {
            println!("error")
        }
        PageCategory::Header => {
            println!("ok")
        }
        _ => {}
    }
    println!("{}", c);
    assert_eq!(format!("{}", c), "HEADER");

    let c = PageCategory::Internal;
    println!("{}", c);
    assert_eq!(format!("{}", c), "INTERNAL");
    assert_eq!(format!("{:?}", c), "INTERNAL");
}

pub struct BTreeBasePage {
    pid: BTreePageID,
    parent_page_index: usize,
}

impl BTreeBasePage {
    pub fn new(pid: &BTreePageID) -> BTreeBasePage {
        BTreeBasePage {
            pid: pid.clone(),
            parent_page_index: 0,
        }
    }

    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    pub fn get_parent_pid(&self) -> BTreePageID {
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

    pub fn set_parent_pid(&mut self, pid: &BTreePageID) {
        self.parent_page_index = pid.page_index;
    }

    pub fn empty_page_data() -> Vec<u8> {
        let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
        data
    }
}

pub struct BTreeRootPointerPage {
    base: BTreeBasePage,

    // The root_pid in mandatory to avoid a bunch of Option & match
    root_pid: BTreePageID,
}

impl std::ops::Deref for BTreeRootPointerPage {
    type Target = BTreeBasePage;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl std::ops::DerefMut for BTreeRootPointerPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl BTreeRootPointerPage {
    pub fn new(pid: &BTreePageID, bytes: Vec<u8>) -> Self {
        let root_page_index =
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let root_pid = BTreePageID {
            category: PageCategory::Leaf,
            page_index: root_page_index,

            // TODO: set table id
            table_id: 0,
        };
        Self {
            base: BTreeBasePage {
                pid: pid.clone(),
                parent_page_index: 0,
            },

            root_pid,
        }
    }

    pub fn get_root_pid(&self) -> BTreePageID {
        self.root_pid
    }

    pub fn set_root_pid(&mut self, pid: &BTreePageID) {
        self.root_pid = *pid;
    }
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
        write!(
            f,
            "<BTreePageID, catagory: {}, page_index: {}, table_id: {}>",
            self.category, self.page_index, self.table_id,
        )
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
}

pub fn empty_page_data() -> Vec<u8> {
    let data: Vec<u8> = vec![0; BufferPool::get_page_size()];
    data
}
