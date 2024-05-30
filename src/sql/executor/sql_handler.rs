use log::{debug, info};
use sqlparser::{
    ast::{ColumnOption, Statement},
    dialect::GenericDialect,
    parser::Parser,
};

use super::stream::Stream;
use crate::{
    error::SmallError,
    sql::{executor::select::handle_select, session::QueryResult},
    storage::table_schema::{Field, Type},
    transaction::Transaction,
    BTreeTable, TableSchema,
};

pub fn handle_sql(tx: &Transaction, sql: &str) -> Result<QueryResult, SmallError> {
    info!("Query: {}", sql);

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    info!("AST: {:?}", ast);

    let statement = &ast[0];

    match statement {
        Statement::CreateTable { name, columns, .. } => {
            info!("name: {:?}", name);
            info!("columns: {:?}", columns);

            let table_name = name.to_string();
            info!("name: {:?}", table_name);

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

            let schema = TableSchema::new(fields);

            let _table = BTreeTable::new(&table_name, None, &schema);
        }
        Statement::Query(query) => {
            match query.body.as_ref() {
                sqlparser::ast::SetExpr::Select(select) => {
                    info!("projection: {:?}", select.projection);
                    info!("from: {:?}", select.from);
                    let stream = handle_select(tx, select)?;
                    return collect_result(stream);
                }
                _ => {
                    todo!()
                }
            }

            todo!()
        }
        _ => {
            todo!()
        }
    }

    todo!()
}

fn collect_result(mut stream: Box<dyn Stream>) -> Result<QueryResult, SmallError> {
    loop {
        if let Some(batch) = stream.next_batch()? {}
    }

    todo!()
}
