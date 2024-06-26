use log::info;
use sqlparser::ast::TableWithJoins;

use crate::{
    error::SmallError, sql::executor::stream::TableStream, transaction::Transaction,
    utils::HandyRwLock, Database,
};

pub fn handle_from(
    tx: &Transaction,
    from: &Vec<TableWithJoins>,
) -> Result<Box<TableStream>, SmallError> {
    let first_from = &from[0];

    if first_from.joins.len() == 0 {
        match &first_from.relation {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                info!("=====");
                info!("handle_from: {:?}", name);
                info!("handle_from: {:?}", alias);
                info!("=====");

                let idents = &name.0;
                if idents.len() == 2 {
                    // find the schema
                    let schema_name = &idents[0].value;
                    let schema = Database::catalog().search_schema(schema_name).unwrap();

                    // find the table
                    let table_name = &idents[1].value;
                    let table = schema.rl().search_table(table_name).unwrap();

                    info!("schema_name: {:?}", schema.rl().name);
                    info!("table_name: {:?}", table.rl().name);

                    let stream = TableStream::new(tx, table);
                    return Ok(Box::new(stream));
                }
            }
            _ => {
                unimplemented!();
            }
        }

        unimplemented!();
    }

    unimplemented!();
}
