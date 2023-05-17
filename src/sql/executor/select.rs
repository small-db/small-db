use sqlparser::ast::Select;

use crate::{error::SmallError, sql::executor::from::handle_from};

use super::expr_state::ExprState;

pub fn handle_select(select: &Select) -> Result<ExprState, SmallError> {
    let join_node = handle_from(&select.from)?;

    todo!()
}
