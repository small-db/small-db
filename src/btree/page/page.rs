use crate::btree::tuple::TupleScheme;

use super::BTreePageID;

pub trait BTreePage {
    fn new(
        pid: &BTreePageID,
        bytes: Vec<u8>,
        tuple_scheme: &TupleScheme,
        key_field: usize,
    ) -> Self
    where
        Self: Sized;

    fn get_pid(&self) -> BTreePageID;

    fn get_parent_pid(&self) -> BTreePageID;
    fn set_parent_pid(&mut self, pid: &BTreePageID);
}
