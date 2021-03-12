#[derive(PartialEq, Copy, Clone)]
pub enum PageCategory {
    ROOT,
    INTERNAL,
    LEAF,
    HEADER,
}

// PageID identifies a unique page, and contains the
// necessary metadata
// TODO: PageID must be hashable
#[derive(Copy, Clone)]
pub struct BTreePageID {
    pub category: PageCategory,
}