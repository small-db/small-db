use sqlparser::ast::Select;

use super::stream::Stream;
use crate::{error::SmallError, sql::executor::from::handle_from};

pub fn handle_select(select: &Select) -> Result<Box<dyn Stream>, SmallError> {
    let _join_node = handle_from(&select.from)?;

    todo!()
}
