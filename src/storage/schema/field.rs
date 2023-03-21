use std::fmt::Debug;

use crate::io::{Decodeable, Encodeable, SmallReader};

use super::Type;


#[derive(PartialEq, Debug, Clone)]
pub struct Field {
    pub name: String,
    pub t: Type,
    pub is_primary: bool,
}

impl Field {
    pub fn new(
        field_name: &str,
        field_type: Type,
        is_primary: bool,
    ) -> Field {
        Field {
            t: field_type,
            name: field_name.to_string(),
            is_primary,
        }
    }
}
