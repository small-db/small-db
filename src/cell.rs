use std::any::Any;
//use std::intrinsics::type_id;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Type {
    INT,
    STRING,
}

pub fn get_type_length(t: Type) -> usize {
    match t {
        Type::INT => 4,
        _ => 0,
    }
}

#[derive(PartialEq, Debug)]
pub struct FieldItem {
    pub(crate) field_type: Type,
    pub field_name: String,
}

pub trait Cell: CellClone {
    fn as_any(&self) -> &dyn Any;
}

pub trait CellClone {
    fn clone_box(&self) -> Box<dyn Cell>;
}

impl<T> CellClone for T
    where
        T: 'static + Cell + Clone,
{
    fn clone_box(&self) -> Box<dyn Cell> {
        Box::new(self.clone())
    }
}

// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<dyn Cell> {
    fn clone(&self) -> Box<dyn Cell> {
        self.clone_box()
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct IntCell {
    value: i128,
}

impl IntCell {
    pub(crate) fn new(v: i128) -> IntCell {
        IntCell { value: v }
    }
}

impl Cell for IntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
