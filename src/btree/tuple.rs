use crate::{cell::IntCell, tuple::TupleScheme};

pub struct BTreeTuple {
    scheme: TupleScheme,
    cells: Vec<IntCell>,
}

impl BTreeTuple {
    pub fn new(n: i32, width: i32) -> BTreeTuple {
        let mut cells: Vec<IntCell> = Vec::new();
        for i in 0..width {
            cells.push(IntCell::new(n));
        }

        todo!()
    }

    pub fn get_field(&self, field_index: i32) -> i32 {
        todo!()
    }
}