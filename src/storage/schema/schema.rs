use crate::field::{FieldItem, Type};

#[derive(Debug)]
pub struct Schema {
    pub fields: Vec<FieldItem>,
}

impl Schema {
    pub fn new(fields: Vec<FieldItem>) -> Schema {
        Schema { fields }
    }

    pub fn merge(scheme1: Schema, scheme2: Schema) -> Schema {
        let mut new_scheme = Schema {
            ..Default::default()
        };

        for f in scheme1.fields {
            new_scheme.fields.push(f);
        }
        for f in scheme2.fields {
            new_scheme.fields.push(f);
        }

        new_scheme
    }

    /// get tuple size in bytes
    pub fn get_size(&self) -> usize {
        self.fields.len() * 4
    }
}

impl Clone for Schema {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.to_vec(),
        }
    }
}

impl Default for Schema {
    fn default() -> Schema {
        Schema { fields: Vec::new() }
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        let matching = self
            .fields
            .iter()
            .zip(&other.fields)
            .filter(|&(a, b)| a == b)
            .count();
        self.fields.len() == matching
    }
}

pub fn small_int_schema(width: usize, name_prefix: &str) -> Schema {
    let mut fields: Vec<FieldItem> = Vec::new();
    for i in 0..width {
        let field = FieldItem {
            field_name: format!("{}-{}", name_prefix, i),
            field_type: Type::INT,
        };
        fields.push(field);
    }

    Schema { fields: fields }
}
