use arrow_flight::FlightDescriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use futures::TryStreamExt;
use std::hint::black_box;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

use mesodb_core::config::Config;
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;
use mesodb_core::wire::WireBatchBuilder;
use mesodb_server::flight::MesoFlightServer;

async fn setup_bench_server() -> (
    tokio::task::JoinHandle<()>,
    FlightServiceClient<Channel>,
    Arc<MesoDB>,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut schema = SchemaMap::new();
    schema.add_attribute(":bench/name", ValueType::String, false);
    schema.add_attribute(":bench/value", ValueType::Int64, false);

    let db =
        Arc::new(MesoDB::open(dir.path().join("bench.db"), schema, Config::default()).unwrap());
    let flight_service = MesoFlightServer::new(db.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stream = TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight_service))
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let url = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = FlightServiceClient::new(channel);

    (server_handle, client, db)
}

fn bench_flight_ipc(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server_handle, client, _db) = rt.block_on(setup_bench_server());

    let mut group = c.benchmark_group("Arrow Flight IPC");

    // ---------------------------------------------------------
    // BENCHMARK 1: Ingestion (do_put)
    // ---------------------------------------------------------
    group.bench_function("do_put_1000_facts", |b| {
        b.to_async(&rt).iter(|| async {
            let mut builder = WireBatchBuilder::new(1000);
            for i in 0..1000 {
                builder
                    .append(&Fact {
                        e: i as u64,
                        ident: ":bench/value".into(),
                        v: Value::Int64(i as i64),
                        op: true,
                        cas_old_v: None,
                        valid_time: None,
                    })
                    .unwrap();
            }

            let batch = builder.finish().unwrap();
            let batch_stream = futures::stream::iter(vec![Ok(batch)]);
            let flight_data_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
                .build(batch_stream)
                .map(|res| res.expect("Flight encoding failed"));

            let mut client_clone = client.clone();
            let response = client_clone.do_put(flight_data_stream).await.unwrap();
            let mut result_stream = response.into_inner();
            result_stream.message().await.unwrap(); // Consume the ACK
        });
    });

    // ---------------------------------------------------------
    // BENCHMARK 2: Querying (get_flight_info + do_get)
    // ---------------------------------------------------------

    // Pre-populate some data so the query actually has work to do
    rt.block_on(async {
        let mut builder = WireBatchBuilder::new(100);
        for i in 0..100 {
            builder
                .append(&Fact {
                    e: i as u64,
                    ident: ":bench/value".into(),
                    v: Value::Int64(i as i64),
                    op: true,
                    cas_old_v: None,
                    valid_time: None,
                })
                .unwrap();
        }
        let batch = builder.finish().unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]);
        let fd_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .build(stream)
            .map(|r| r.unwrap());
        let mut init_client = client.clone();
        init_client
            .do_put(fd_stream)
            .await
            .unwrap()
            .into_inner()
            .message()
            .await
            .unwrap();
    });

    let query_dto = mesodb_server::dto::QueryRequest {
        query: r#"[:find (sum ?val) :where [?e :bench/value ?val]]"#.to_string(),
        as_of: None,
        rules: None,
        format: Default::default(),
    };
    let query_bytes = serde_json::to_vec(&query_dto).unwrap();

    group.bench_function("do_get_sum_aggregation", |b| {
        b.to_async(&rt).iter(|| async {
            let mut client_clone = client.clone();
            let descriptor = FlightDescriptor::new_cmd(query_bytes.clone());

            // Handshake
            let info = client_clone
                .get_flight_info(descriptor)
                .await
                .unwrap()
                .into_inner();
            let ticket = info.endpoint[0].ticket.clone().unwrap();

            // Fetch
            let flight_stream = client_clone.do_get(ticket).await.unwrap().into_inner();
            let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
            );

            while let Some(batch) = decoder.next().await {
                black_box(batch.unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_flight_ipc);
criterion_main!(benches);
