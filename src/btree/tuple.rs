use crate::{field::IntField, tuple::TupleScheme};

pub struct BTreeTuple {
    scheme: TupleScheme,
    cells: Vec<IntField>,
}

impl BTreeTuple {
    pub fn new(n: i32, width: i32) -> BTreeTuple {
        let mut cells: Vec<IntField> = Vec::new();
        for _i in 0..width {
            cells.push(IntField::new(n));
        }

        todo!()
    }

    pub fn get_field(&self, _field_index: i32) -> i32 {
        todo!()
    }
}
