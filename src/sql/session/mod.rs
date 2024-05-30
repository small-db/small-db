use super::executor::{sql_handler::handle_sql, stream::Batch};
use crate::{
    error::SmallError,
    storage::{table_schema::Field, tuple::Tuple},
    transaction::Transaction,
};

pub struct Session {}

pub struct QueryResult {
    pub data: Vec<Tuple>,
}

impl QueryResult {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn push_batch(&mut self, batch: &Batch) {
        self.data.extend(batch.rows.clone());
    }
}

impl futures_core::stream::Stream for QueryResult {
    type Item = pgwire::error::PgWireResult<pgwire::messages::data::DataRow>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl Session {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&mut self, tx: &Transaction, sql_text: &str) -> Result<QueryResult, SmallError> {
        handle_sql(tx, sql_text)
    }
}
