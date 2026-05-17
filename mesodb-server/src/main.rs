// mesodb-server/src/main.rs

use axum::{Router, routing::post};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use mesodb_core::config::Config;
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};

mod dto;
mod handlers;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    color_eyre::install()?;

    tracing::info!("Initializing MesoDB Server Cluster state...");

    // Setup base schema
    let mut schema = SchemaMap::new();
    schema.add_attribute(":sensor/id", ValueType::Int64, false);
    schema.add_attribute(":sensor/reading", ValueType::Float64, false);
    schema.add_attribute(":user/name", ValueType::String, false);
    schema.add_attribute(":user/email", ValueType::String, true);

    // Initialize DB
    let db_path = PathBuf::from("./data/prod_server.db");
    let config = Config::default();
    let db = Arc::new(MesoDB::open(db_path, schema, config)?);

    // Build the router with direct State attachment
    let app = Router::new()
        .route("/transact", post(handlers::handle_transact))
        .route("/query", post(handlers::handle_query))
        .with_state(db);

    // Bind and serve using standard tokio tools
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("MesoDB Server online and running at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
