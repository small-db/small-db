use log::{debug, info};
use sqlparser::ast::TableWithJoins;

use super::expr_state::Stream;
use crate::{error::SmallError, sql::executor::join::handle_join};

pub fn handle_from(from: &Vec<TableWithJoins>) -> Result<Stream, SmallError> {
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

                    // find the table
                    let table_name = &idents[1].value;

                    debug!("schema_name: {:?}", schema_name);
                    debug!("table_name: {:?}", table_name);

                    // return the stream of the table
                }
            }
            _ => {
                unimplemented!();
            }
        }

        return Ok(Stream::new());
    }

    unimplemented!();

    let first_join = &first_from.joins[0];
    return handle_join(first_join);
}
