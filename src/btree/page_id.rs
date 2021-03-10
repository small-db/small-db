#[derive(PartialEq, Copy, Clone)]
pub enum PageCategory {
    ROOT,
    INTERNAL,
    LEAF,
    HEADER,
}

#[derive(Copy, Clone)]
pub struct BTreePageID {
    pub category: PageCategory,
}