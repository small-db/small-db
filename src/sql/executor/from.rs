use log::info;
use sqlparser::ast::TableWithJoins;

use super::expr_state::ExprState;
use crate::{error::SmallError, sql::executor::join::handle_join};

pub fn handle_from(from: &Vec<TableWithJoins>) -> Result<ExprState, SmallError> {
    let first_from = &from[0];
    info!("=====");
    info!("handle_from: {:?}", first_from.relation);
    info!("=====");
    info!("handle_from: {:?}", first_from.joins);

    let first_join = &first_from.joins[0];
    return handle_join(first_join);
}
