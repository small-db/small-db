use super::{Field, Type};

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

// Constructors
impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn for_schema_table() -> Self {
        Self {
            fields: vec![
                Field::new("table_id", Type::Int64, true),
                Field::new("table_name", Type::Bytes(20), false),
                Field::new("field_name", Type::Bytes(20), false),
                Field::new("field_type", Type::Bytes(10), false),
                Field::new("is_primary", Type::Bool, false),
            ],
        }
    }

    pub fn small_int_schema(width: usize) -> Self {
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..width {
            let field = Field {
                name: format!("int-column-{}", i),
                t: Type::Int64,
                is_primary: i == 0,
            };

            fields.push(field);
        }

        Self { fields }
    }
}

impl Schema {
    /// get tuple size in bytes
    pub fn get_size(&self) -> usize {
        let mut size = 0;
        for field in &self.fields {
            size += field.t.size();
        }
        size
    }

    pub fn get_key_field(&self) -> usize {
        for (i, field) in self.fields.iter().enumerate() {
            if field.is_primary {
                return i;
            }
        }
        panic!("no key field found");
    }
}
