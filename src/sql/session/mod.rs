use super::executor::sql_handler::handle_sql;
use crate::{
    error::SmallError,
    storage::{table_schema::Field, tuple::Tuple},
};

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
