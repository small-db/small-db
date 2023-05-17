use crate::{
    error::SmallError,
    storage::{schema::Field, tuple::Tuple},
};

use super::executor::sql_handler::handle_sql;

pub struct Session {}

pub struct QueryResult {
    pub fields: Vec<Field>,
    pub data: Vec<Tuple>,
}

impl Session {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&mut self, sql_text: &str) -> Result<QueryResult, SmallError> {
        handle_sql(sql_text)
    }
}
