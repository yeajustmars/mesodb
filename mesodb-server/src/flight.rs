// mesodb-server/src/flight.rs

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder,
    flight_service_server::FlightService,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::result::Result;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

use crate::dto::QueryRequest;
use mesodb_core::{
    db::{MesoDB, OutputFormat, QueryOptions},
    wire::parse_wire_batch,
};

pub struct MesoFlightServer {
    db: Arc<MesoDB>,
}

impl MesoFlightServer {
    pub fn new(db: Arc<MesoDB>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl FlightService for MesoFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;

        // 1. Decode the Ticket bytes as our JSON QueryRequest DTO
        let query_req: QueryRequest = serde_json::from_slice(&ticket)
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket format: {}", e)))?;

        let opts = QueryOptions {
            as_of: query_req.as_of,
            rules: query_req.rules,
            format: OutputFormat::Arrow,
            history: false,
        };

        // 2. Execute the query using the core engine (assuming this returns a Vec<RecordBatch>)
        let batches = self
            .db
            .query_with_options(&query_req.query, opts)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {:?}", e)))?;

        if batches.is_empty() {
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        }

        // 3. Extract the schema from the first batch to prime the Flight encoder
        let schema = batches[0].schema();

        // 4. Convert the Vec<RecordBatch> into a Stream
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));

        // 5. Stream the zero-copy payloads using the Arrow Flight Encoder
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|res| {
                res.map_err(|e| Status::internal(format!("Flight encoding error: {:?}", e)))
            });

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();

        // Map the tonic::Status error into an arrow_flight::error::FlightError
        let mapped_stream =
            stream.map(|res| res.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))));

        // FlightRecordBatchStream now receives the exact Error type it demands
        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);

        let mut final_tx_id = 0;
        // TODO: put back
        // let mut total_datoms_written = 0;

        // Iterate through the streamed RecordBatches as they arrive over the network
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result.map_err(|e| {
                Status::internal(format!("Failed to decode Arrow Flight batch: {}", e))
            })?;

            // 1. Zero-copy parse from the Arrow columnar layout into our native Fact structs
            let facts = parse_wire_batch(&batch).map_err(|e| {
                Status::invalid_argument(format!("Wire protocol violation: {:?}", e))
            })?;

            // 2. Commit the transaction to the MesoDB engine
            match self.db.transact(facts).await {
                Ok(report) => {
                    final_tx_id = report.tx_id;
                    // TODO: put back
                    // total_datoms_written += report.datoms_written;
                }
                Err(e) => {
                    return Err(Status::internal(format!("Transaction failed: {:?}", e)));
                }
            }
        }

        // 3. Construct a PutResult acknowledgment containing the final Transaction ID
        let put_result = PutResult {
            app_metadata: final_tx_id.to_be_bytes().to_vec().into(),
        };

        // 4. Return the stream of results (Flight requires a stream, even for single acks)
        let output_stream = futures::stream::iter(vec![Ok(put_result)]);
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not yet implemented"))
    }

    // --- Standard Flight boilerplate we don't strictly need for MesoDB yet ---

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // 1. Extract the JSON payload from the FlightDescriptor command
        let query_req: crate::dto::QueryRequest = serde_json::from_slice(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("Invalid query descriptor: {}", e)))?;

        let opts = mesodb_core::db::QueryOptions {
            as_of: query_req.as_of,
            rules: query_req.rules,
            format: mesodb_core::db::OutputFormat::Arrow,
            history: false,
        };

        // 2. Execute the query to capture the schema natively from DataFusion
        let batches = self
            .db
            .query_with_options(&query_req.query, opts)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {:?}", e)))?;

        let schema = if !batches.is_empty() {
            batches[0].schema()
        } else {
            std::sync::Arc::new(arrow::datatypes::Schema::empty())
        };

        // 3. Serialize the schema to Arrow IPC bytes for the client
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_bytes =
            arrow_flight::FlightData::from(arrow_flight::SchemaAsIpc::new(&schema, &options))
                .data_header;

        // 4. Return the exact same command bytes as the Ticket to execute in do_get
        let ticket = Ticket {
            ticket: descriptor.cmd.clone(),
        };

        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(ticket),
            location: vec![], // Empty location tells the client to stream from this same server
            app_metadata: Default::default(),
            expiration_time: None,
        };

        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            app_metadata: Default::default(),
            ordered: false,
        };

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_flight::FlightDescriptor;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::FlightServiceServer;
    use futures::TryStreamExt;
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

    async fn setup_test_server() -> (tokio::task::JoinHandle<()>, FlightServiceClient<Channel>) {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let db =
            Arc::new(MesoDB::open(dir.path().join("test.db"), schema, Config::default()).unwrap());
        let flight_service = MesoFlightServer::new(db);

        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpListenerStream::new(listener);

        // Spawn the gRPC server in the background
        let server_handle = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(flight_service))
                .serve_with_incoming(stream)
                .await
                .unwrap();
        });

        // Wait a tiny bit to ensure the server is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let url = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(url)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = FlightServiceClient::new(channel);

        (server_handle, client)
    }

    #[tokio::test]
    async fn test_flight_e2e_put_and_get() {
        let (_server_handle, mut client) = setup_test_server().await;

        // ==========================================
        // 1. INGESTION (do_put)
        // ==========================================
        let mut builder = WireBatchBuilder::new(2);

        let fact_name = Fact {
            e: 1,
            ident: ":user/name".into(),
            v: Value::String("Alice".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        };
        builder.append(&fact_name).unwrap();

        let fact_age = Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: true,
            cas_old_v: None,
            valid_time: None,
        };
        builder.append(&fact_age).unwrap();

        let record_batch = builder.finish().unwrap();

        // Arrow's FlightDataEncoder handles the IPC serialization for the wire
        let batch_stream = futures::stream::iter(vec![Ok(record_batch)]);
        let flight_data_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .build(batch_stream)
            // The tonic client strictly expects FlightData, so we unwrap the encode result
            .map(|res| res.expect("Flight encoding failed"));

        // Execute the write operation over the network
        let response = client.do_put(flight_data_stream).await.unwrap();

        // Ensure we got an acknowledgment back from the server
        let mut put_result_stream = response.into_inner();
        let put_result = put_result_stream
            .message()
            .await
            .unwrap()
            .expect("Expected PutResult");
        assert!(
            !put_result.app_metadata.is_empty(),
            "Tx ID should be returned in metadata"
        );

        // ==========================================
        // 2. QUERYING (get_flight_info -> do_get)
        // ==========================================
        let query_dto = crate::dto::QueryRequest {
            query: r#"[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age]]"#
                .to_string(),
            as_of: None,
            rules: None,
            format: Default::default(), // Added to satisfy the DTO struct
        };

        let query_json = serde_json::to_vec(&query_dto).unwrap();

        // A. Handshake: Get schema and ticket
        let descriptor = FlightDescriptor::new_cmd(query_json.clone());
        let info_response = client
            .get_flight_info(descriptor)
            .await
            .unwrap()
            .into_inner();
        let endpoint = info_response.endpoint[0].clone();
        let ticket = endpoint.ticket.unwrap();

        // B. Execution: Stream the zero-copy Arrow data back
        let flight_stream = client.do_get(ticket).await.unwrap().into_inner();

        let mut received_batches = vec![];
        // Decode the incoming FlightData back into RecordBatches
        let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        while let Some(batch) = decoder.next().await {
            received_batches.push(batch.unwrap());
        }

        assert_eq!(
            received_batches.len(),
            1,
            "Should receive exactly one Result RecordBatch"
        );
        let result_batch = &received_batches[0];

        // Verify the projected schema structure
        assert_eq!(
            result_batch.schema().fields().len(),
            2,
            "Query requested ?name and ?age"
        );
        assert_eq!(result_batch.num_rows(), 1, "Should be one row for Alice");

        // Verify the exact zero-copy values returned natively from DataFusion
        let name_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let age_col = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(age_col.value(0), 30);
    }
}
