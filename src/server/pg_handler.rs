use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pgwire::{
    api::{
        query::SimpleQueryHandler,
        results::{QueryResponse, Response},
        ClientInfo,
    },
    error::PgWireResult,
};

use crate::{sql::session::Session, transaction::Transaction};

pub struct PostgresHandler {
    pub session: Arc<Mutex<Session>>,
}

impl PostgresHandler {
    pub fn new(session: Arc<Mutex<Session>>) -> Self {
        Self { session }
    }
}

#[async_trait]
impl SimpleQueryHandler for PostgresHandler {
    async fn do_query<'b, C>(&self, _client: &C, query: &'b str) -> PgWireResult<Vec<Response<'b>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut session = self.session.lock().unwrap();

        let tx = Transaction::new();

        let result = session
            .execute(&tx, query)
            .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

        let field_defs = Vec::new();

        let query_response = QueryResponse::new(Arc::new(field_defs), result);
        let response = Response::Query(query_response);

        unimplemented!()
    }
}
