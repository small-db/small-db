use std::sync::{Arc, Mutex};

use pgwire::{
    api::{
        auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, MakeHandler,
        StatelessMakeHandler,
    },
    tokio::process_socket,
};
use small_db::{
    server::pg_handler::{self, PostgresHandler},
    sql::session::{self, Session},
};
use tokio::net::TcpListener;

#[tokio::main]
pub async fn main() {
    let session = Arc::new(Mutex::new(Session::new()));
    let pg_handler = PostgresHandler::new(session);

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(pg_handler)));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
