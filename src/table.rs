use crate::row::RowScheme;

pub trait Table {
    fn get_row_scheme(&self) -> &RowScheme;
    fn get_id(&self) -> i32;
}

pub struct SkeletonTable {
    pub table_id: i32,
    pub row_scheme: RowScheme,
}

impl Table for SkeletonTable {
    fn get_row_scheme(&self) -> &RowScheme {
        &self.row_scheme
    }

    fn get_id(&self) -> i32 {
        self.table_id
    }
}
