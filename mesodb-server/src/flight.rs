// mesodb-server/src/flight.rs

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as ArrowFlightResult,
    SchemaAsIpc, SchemaResult, Ticket, decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService,
};
use futures::{Stream, StreamExt, stream};
use std::{pin::Pin, result::Result as StdResult, sync::Arc};
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
        Pin<Box<dyn Stream<Item = StdResult<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = StdResult<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = StdResult<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = StdResult<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = StdResult<ArrowFlightResult, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = StdResult<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = StdResult<FlightData, Status>> + Send + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> StdResult<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;

        let query_req: QueryRequest = serde_json::from_slice(&ticket)
            .map_err(|e| Status::invalid_argument(format!("Malformed query ticket: {}", e)))?;

        let opts = QueryOptions {
            as_of: query_req.as_of,
            rules: query_req.rules,
            format: OutputFormat::Arrow,
            history: false,
        };

        let batches = self
            .db
            .query_with_options(&query_req.query, opts)
            .await
            .map_err(|e| Status::internal(format!("DataFusion execution panic: {:?}", e)))?;

        if batches.is_empty() {
            return Ok(Response::new(Box::pin(stream::empty())));
        }

        let schema = batches[0].schema();
        let batch_stream = stream::iter(batches.into_iter().map(Ok));

        // Stream Arrow FlightData frames directly over gRPC transport
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|res| res.map_err(|e| Status::internal(format!("IPC Encoding failure: {:?}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();

        let mapped_stream =
            stream.map(|res| res.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))));

        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);
        let mut final_tx_id = 0;

        // Stream processing: Map incoming network buffers directly to the core Transactor
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result
                .map_err(|e| Status::internal(format!("Arrow IPC Decapsulation error: {}", e)))?;

            let facts = parse_wire_batch(&batch).map_err(|e| {
                Status::invalid_argument(format!("Wire-to-AST parsing violation: {:?}", e))
            })?;

            match self.db.transact(facts).await {
                Ok(report) => {
                    final_tx_id = report.tx_id;
                }
                Err(e) => {
                    return Err(Status::internal(format!("Core Engine rejection: {:?}", e)));
                }
            }
        }

        let put_result = PutResult {
            app_metadata: final_tx_id.to_be_bytes().to_vec().into(),
        };

        Ok(Response::new(Box::pin(stream::iter(vec![Ok(put_result)]))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> StdResult<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        let query_req: QueryRequest = serde_json::from_slice(&descriptor.cmd).map_err(|e| {
            Status::invalid_argument(format!("Descriptor CMD must be valid JSON: {}", e))
        })?;

        let opts = QueryOptions {
            as_of: query_req.as_of,
            rules: query_req.rules,
            format: OutputFormat::Arrow,
            history: false,
        };

        // Execute natively to allow DataFusion to infer the schema
        let batches = self
            .db
            .query_with_options(&query_req.query, opts)
            .await
            .map_err(|e| Status::internal(format!("DataFusion Planning failure: {:?}", e)))?;

        let schema = if !batches.is_empty() {
            batches[0].schema()
        } else {
            Arc::new(arrow::datatypes::Schema::empty())
        };

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_bytes = FlightData::from(SchemaAsIpc::new(&schema, &options)).data_header;

        let ticket = Ticket {
            ticket: descriptor.cmd.clone(),
        };

        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
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

    // --- Unimplemented Boilerplate ---

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> StdResult<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> StdResult<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> StdResult<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> StdResult<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}
