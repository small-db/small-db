use super::{Field, Type};

#[derive(Debug)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Schema {
        Schema { fields }
    }

    pub fn for_schema_table() -> Schema {
        Schema {
            fields: vec![
                Field::new("table_id", Type::Int64, true),
                Field::new("table_name", Type::Bytes(255), false),
                Field::new("field_name", Type::Bytes(10), false),
                Field::new("field_type", Type::Bytes(10), false),
                Field::new("is_primary", Type::Bool, false),
            ],
        }
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
    let mut fields: Vec<Field> = Vec::new();
    for i in 0..width {
        let field = Field {
            name: format!("{}-{}", name_prefix, i),
            t: Type::Int64,
            is_primary: false,
        };
        fields.push(field);
    }

    Schema { fields: fields }
}
