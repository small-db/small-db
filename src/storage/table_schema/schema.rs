use super::{Field, Type};

#[derive(Debug, Clone)]
pub struct TableSchema {
    fields: Vec<Field>,
}

// Constructors
impl TableSchema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn for_table_schema() -> Self {
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

    pub fn for_schema() -> Self {
        Self {
            fields: vec![
                Field::new("schema_id", Type::Int64, true),
                Field::new("schema_name", Type::Bytes(20), false),
            ],
        }
    }

    pub fn small_int_schema(width: usize) -> Self {
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..width {
            let field = Field::new(&format!("int-column-{}", i), Type::Int64, i == 0);

            fields.push(field);
        }

        Self::new(fields)
    }
}

impl TableSchema {
    /// Get tuple size in bytes.
    pub fn get_disk_size(&self) -> usize {
        let mut size = 0;
        for field in self.get_fields() {
            size += field.get_type().get_disk_size();
        }
        size
    }

    /// Get the position of the key field.
    pub fn get_key_pos(&self) -> usize {
        for (i, field) in self.get_fields().iter().enumerate() {
            if field.is_primary {
                return i;
            }
        }
        panic!("no key field found");
    }

    pub fn get_field_pos(&self, field_name: &str) -> usize {
        for (i, field) in self.get_fields().iter().enumerate() {
            if field.name == field_name {
                return i;
            }
        }
        panic!("no field found");
    }

    pub fn get_fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn get_pkey(&self) -> &Field {
        for field in self.get_fields() {
            if field.is_primary {
                return field;
            }
        }
        panic!("no key field found");
    }
}
