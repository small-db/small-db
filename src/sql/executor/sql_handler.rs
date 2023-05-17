use log::info;
use sqlparser::{
    ast::{ColumnOption, Statement},
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{
    error::SmallError,
    sql::{
        executor::{expr_state, select::handle_select},
        session::QueryResult,
    },
    storage::schema::{Field, Type},
    BTreeTable, Schema,
};

use super::expr_state::ExprState;

pub fn handle_sql(sql: &str) -> Result<QueryResult, SmallError> {
    info!("Query: {}", sql);

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);

    println!("AST: {:?}", ast[0]);

    let statement = &ast[0];

    match statement {
        Statement::CreateTable { name, columns, .. } => {
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

            let _table = BTreeTable::new(&table_name, None, &schema);
        }
        Statement::Query(query) => {
            match query.body.as_ref() {
                sqlparser::ast::SetExpr::Select(select) => {
                    info!("projection: {:?}", select.projection);
                    info!("from: {:?}", select.from);
                    let expr_state = handle_select(select)?;
                    return Ok(collect_result(&expr_state));
                }
                _ => {
                    todo!()
                }
            }

            // our target:
            // {
            //     let table: BTreeTable;
            //     let cell: Cell;

            //     let predicate = Predicate::new(Op::Equals, &cell);
            //     let tx = Transaction::new();
            //     let mut it = BTreeTableSearchIterator::new(&tx, &table, &predicate);
            //     let target = it.next().unwrap();
            //     table.delete_tuple(&tx, &target).unwrap();

            //     tx.commit().unwrap();
            // }

            todo!()
        }
        _ => {
            todo!()
        }
    }

    todo!()
}

fn collect_result(expr_state: &ExprState) -> QueryResult {
    todo!()
}
