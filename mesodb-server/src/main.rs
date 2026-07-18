// mesodb-server/src/main.rs

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{Router, routing::post};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server as TonicServer;

use mesodb_core::config::Config;
use mesodb_core::db::MesoDB;
use mesodb_core::schema::SchemaMap;
use mesodb_server::{flight::MesoFlightServer, handlers};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    color_eyre::install()?;

    tracing::info!("Initializing MesoDB Server Cluster state...");

    // Setup base schema
    let schema = SchemaMap::new();

    // Initialize DB
    // TODO: fix hardcoded config path
    let db_path = PathBuf::from("./data/prod_server.db");
    let config = Config::default();
    let db = Arc::new(MesoDB::open(db_path, schema, config)?);

    // ==========================================
    // 1. Setup Axum HTTP Server
    // ==========================================
    let app = Router::new()
        .route("/schema", post(handlers::handle_schema))
        .route("/query", post(handlers::handle_query))
        .route("/transact", post(handlers::handle_transact))
        .with_state(db.clone());

    let http_addr = SocketAddr::from(([127, 0, 0, 1], 8000));
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

    // ==========================================
    // 2. Setup Tonic Arrow Flight gRPC Server
    // ==========================================
    let flight_addr = "127.0.0.1:50051".parse()?;
    let flight_server = MesoFlightServer::new(db.clone());
    let flight_svc = FlightServiceServer::new(flight_server);

    tracing::info!("MesoDB HTTP API online at http://{}", http_addr);
    tracing::info!("MesoDB Flight RPC online at grpc://{}", flight_addr);
    println!("MesoDB is fully online. (HTTP: 8000, Flight: 50051)");

    // ==========================================
    // 3. Run Both Servers Concurrently
    // ==========================================
    let http_future = axum::serve(http_listener, app);
    let grpc_future = TonicServer::builder()
        .add_service(flight_svc)
        .serve(flight_addr);

    // If either server crashes or exits, the select! macro will return and shut down the process.
    tokio::select! {
        res = http_future.into_future() => {
            tracing::error!("HTTP server exited unexpectedly: {:?}", res);
        }
        res = grpc_future => {
            tracing::error!("gRPC server exited unexpectedly: {:?}", res);
        }
    }

    Ok(())
}
