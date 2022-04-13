use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::MessageHeader;
use arrow::ipc::reader::StreamReader;
use tonic::transport::channel::Channel;
use tonic::client::Grpc;
use std::{str};
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::util::display;

use arrow_flight::{
    flight_service_client::FlightServiceClient,
    Criteria,
    Action, ActionType, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, flight_descriptor, utils::flight_data_to_arrow_batch,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut client = FlightServiceClient::connect("grpc://0.0.0.0:8815").await?;

    // let mut criteria = Criteria::default();
    // criteria.expression = "point_cloud2.dataset".as_bytes().to_vec();
    println!("get point_cloud2.dataset flight_descriptor");
    let criteria = Criteria {expression: "point_cloud2.dataset".as_bytes().to_vec()};
    let list_flights_response = client.list_flights(criteria).await?;

    let mut stream = list_flights_response.into_inner();

    while let Some(flight_info) = stream.message().await? {
        let flight_descriptor = flight_info.flight_descriptor.unwrap();
        println!("flight_info - flight_descriptor.path:{:?} rows: {} size: {}", flight_descriptor.path, flight_info.total_records, flight_info.total_bytes);
    }
    println!("====");

    println!("list all flight_descriptors for parquets");


    let criteria = Criteria::default();
    let list_flights_response = client.list_flights(criteria).await?;

    let mut stream = list_flights_response.into_inner();

    while let Some(flight_info) = stream.message().await? {
        let flight_descriptor = flight_info.flight_descriptor.unwrap();
        println!("flight_info - flight_descriptor.path:{:?} rows: {} size: {}", flight_descriptor.path, flight_info.total_records, flight_info.total_bytes);
    }
    println!("====");

    let path = vec!("ubx_nav_hp_pos_llh_20220404150357.parquet".to_string());
    let f1_descriptor = FlightDescriptor::new_path(path);

    let f1 = client.get_flight_info(f1_descriptor).await?.into_inner();
    // println!("path: {} rows: {} size: {}", f1.flight_descriptor.unwrap().path[0], f1.total_records, f1.total_bytes);
    // println!("== schema ==");
    // println!("{}", String::from_utf8_lossy(&f1.to_string()));
    // println!("====");
    println!("{}", &f1);
    println!("====");
    let schema_ref= Arc::new(Schema::try_from(f1.clone()).unwrap());
    println!("schema: {}", schema_ref.clone());
    println!("====");

    match &f1.endpoint[0].ticket {
        Some(ticket) => {
            let mut stream = client.do_get(ticket.clone()).await?.into_inner();
            let mut dictionaries_by_field = Vec::<Option::<ArrayRef>>::new();


            while let Some(flight_data) = stream.message().await?{
                println!("{}", flight_data);
                // println!("{}", String::from_utf8_lossy(&flight_data.data_header));
                let message = arrow::ipc::root_as_message(&flight_data.data_header[..]).map_err(|err| {
                    ArrowError::ParseError(format!("Unable to get root as message: {:?}", err))
                })?;

                println!("header_type: {:?}", message.header_type());

                match message.header_type() {
                    MessageHeader::Schema => {
                        println!("Schema")
                    },
                    MessageHeader::RecordBatch => {
                        println!("RecordBatch");
                        let rb = flight_data_to_arrow_batch(&flight_data, schema_ref.clone(), &dictionaries_by_field).unwrap();
                        println!("rb num_columns: {} num_rows: {}", rb.num_columns(), rb.num_rows());

                        println!("rb schema fields: {:?}", rb.schema().fields());
                        // println!("rb columns: {:?}", rb.columns());
                        // print_batches(rb);
                        // for i in 0..rb.num_columns() {
                        //     let col = rb.column(i);

                        // }
                        // println!("rb: {:?}", rb);
                    },
                    _ => {
                        println!("header_type: {:?} not implemented", message.header_type());
                    }
                }


                // flight_data.

            }
        },
        None => {
            println!("no ticket");
        }
    }

    println!("====");

    Ok(())
}
