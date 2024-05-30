use super::executor::{sql_handler::handle_sql, stream::Batch};
use crate::{
    error::SmallError,
    io::Encodeable,
    storage::{table_schema::Field, tuple::Tuple},
    transaction::Transaction,
};

pub struct Session {}

pub struct QueryResult {
    pub data: Vec<Tuple>,
    cursor: usize,
}

impl QueryResult {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            cursor: 0,
        }
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
        if self.cursor >= self.data.len() {
            return std::task::Poll::Ready(None);
        }

        let tuple = &self.data[self.cursor];
        let mut bytes_list: Vec<Option<bytes::Bytes>> = Vec::new();
        for cell in tuple.get_cells() {
            let bytes = cell.to_bytes();
            let v = bytes::Bytes::copy_from_slice(&bytes);
            bytes_list.push(Some(v));
        }

        let data_row = pgwire::messages::data::DataRow::new(bytes_list);
        return std::task::Poll::Ready(Some(Ok(data_row)));
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
