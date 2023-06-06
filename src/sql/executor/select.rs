use sqlparser::ast::Select;

use super::expr_state::ExprState;
use crate::{error::SmallError, sql::executor::from::handle_from};

pub fn handle_select(select: &Select) -> Result<ExprState, SmallError> {
    let _join_node = handle_from(&select.from)?;

    todo!()
}
