use log::info;
use sqlparser::ast::Join;

use super::stream::Stream;
use crate::error::SmallError;

pub fn handle_join(join: &Join) -> Result<Stream, SmallError> {
    info!("=====");
    info!("handle_join: {:?}", join.relation);
    info!("=====");
    info!("handle_join: {:?}", join.join_operator);

    match &join.join_operator {
        sqlparser::ast::JoinOperator::LeftOuter(constrains) => {
            info!("handle_join: LeftOuter, constrains: {:?}", constrains);
        }
        _ => {
            todo!()
        }
    }

    todo!()
}
