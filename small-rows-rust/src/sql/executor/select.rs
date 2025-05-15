use sqlparser::ast::Select;

use super::stream::Stream;
use crate::{error::SmallError, sql::executor::from::handle_from, transaction::Transaction};

pub fn handle_select(tx: &Transaction, select: &Select) -> Result<Box<dyn Stream>, SmallError> {
    let stream = handle_from(tx, &select.from)?;
    return Ok(stream);
}
