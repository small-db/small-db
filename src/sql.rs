use sqlparser::ast::ColumnOption;
use sqlparser::ast::Statement::CreateTable;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::storage::schema::{Field, Type};
use crate::{BTreeTable, Schema};

pub fn handle_sql(sql: &str) {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);

    println!("AST: {:?}", ast[0]);

    let statement = &ast[0];

    match statement {
        CreateTable { name, columns, .. } => {
            println!("name: {:?}", name);
            println!("columns: {:?}", columns);

            let table_name = name.to_string();
            println!("name: {:?}", table_name);

            let mut fields: Vec<Field> = Vec::new();
            for column in columns {
                let is_pkey = column.options.iter().any(|c| match c.option {
                    ColumnOption::Unique { is_primary: true } => true,
                    _ => false,
                });

                let field_type = match &column.data_type {
                    sqlparser::ast::DataType::Integer(_) => Type::Int64,
                    sqlparser::ast::DataType::Varchar(_) => Type::Bytes(20),
                    _ => Type::Int64,
                };

                let field = Field::new(&column.name.to_string(), field_type, is_pkey);

                fields.push(field);
            }

            let schema = Schema::new(fields);

            let table = BTreeTable::new(&table_name, None, &schema);
        }
        _ => {
            println!("not create table");
        }
    }
}
