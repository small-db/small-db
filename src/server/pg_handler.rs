use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use log::info;
use pgwire::{
    api::{query::SimpleQueryHandler, results::Response, ClientInfo},
    error::PgWireResult,
};

use crate::sql::session::Session;

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
        info!("Query: {}", query);
        unimplemented!()
    }
}
