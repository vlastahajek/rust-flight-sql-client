use arrow_array::{RecordBatch};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightInfo};
use std::{sync::Arc, time::Duration};
use arrow_schema::{ArrowError, Schema};
//use futures::stream::TryStreamExt;
use futures_util::stream::TryStreamExt;
use arrow_ipc::writer::FileWriter;
use std::time::Instant;
use std::ops::Deref;

async fn execute_flight(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<Vec<RecordBatch>,ArrowError> {
    let schema = Arc::new(Schema::try_from(info.clone()).expect("schema"));
    let mut batches = Vec::with_capacity(info.endpoint.len() + 1);
    batches.push(RecordBatch::new_empty(schema));

    println!("decoded schema");

    for endpoint in info.endpoint {
        let Some(ticket) = &endpoint.ticket else {
            panic!("did not get ticket");
        };
        let flight_data_result = client.do_get(ticket.clone()).await;
        let flight_data = match flight_data_result {
            Ok(data) => data,
            Err(e) => {
                println!("Failed to get flight data, ({:?})", e);
                return Err(e);
            }
        };
        let mut flight_data: Vec<_> = flight_data.try_collect().await.expect("data");
        batches.append(&mut flight_data);
    }

    println!("received data");

    Ok(batches)
}

fn record_batch_to_buffer(batches: Vec<RecordBatch>) -> Result<Vec<u8>, ArrowError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches.get(0).unwrap().schema();
    let mut fr = FileWriter::try_new(Vec::new(), schema.deref()).map_err(|err| ArrowError::ExternalError(Box::new(err)))?;

    for batch in batches.iter() {
        fr.write(batch).map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
    }
    fr.finish().map_err(|err| ArrowError::ExternalError(Box::new(err)))?; 
    return fr.into_inner();
}   


#[tokio::main]
async fn main() {

    let endpoint_result = Endpoint::new("https://us-east-1-1.aws.cloud2.influxdata.com");
    let endpoint = match endpoint_result {
        Ok(url) => url,
        Err(e) => {
            println!("Failed to create endpoint, ({:?})", e);
            return;
        }
    };
    let endpoint = endpoint
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    // let tls_config = ClientTlsConfig::new();
    // let endpoint_result = endpoint
    //     .tls_config(tls_config);
    // let endpoint = match endpoint_result {
    //     Ok(url) => url,
    //     Err(e) => {
    //         println!("Failed to create endpoint, ({:?})", e);
    //         return;
    //     }
    // };
    
    //let endpoint = Ok(url).tls_config(ClientTlsConfig::new());
    let channel_result = endpoint
        .connect()
        .await;

    let channel = match channel_result {
        Ok(chan) => chan,
        Err(e) => {
            println!("Failed to connect to endpoint, ({:?})", e);
            return;
        }
    };
  
    //endpoint.add

    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token("jGVT56tt_eeD7WDfh0y945R6r54k3XYmdLWLjJU0vpWsJbS7PqbwLFX58vfgucEOx5jr_B9v6Ap-myu4oVnirg==".to_string());
    client.set_header("database", "samans-bucket");
    println!("connected");

    let now = Instant::now();

    let query = "SELECT * FROM win_cpu WHERE time > now() - interval '2 days'";
    let mut prepared_stmt_result = client.prepare(query.to_string(), None).await;
    let mut prepared_stmt = match prepared_stmt_result {
        Ok(stmt) => stmt,
        Err(e) => {
            println!("Failed to prepare statement, ({:?})", e);
            return;
        }
    };
    println!("prepared statement");
    let flight_info_result = prepared_stmt.execute().await;
    let flight_info = match flight_info_result {
        Ok(info) => info,
        Err(e) => {
            println!("Failed to execute flight, ({:?})", e);
            return;
        }
    };
    let batches_result = execute_flight(&mut client, flight_info).await;
    let batches = match batches_result {
        Ok(batches) => batches,
        Err(e) => {
            println!("Failed to execute flight, ({:?})", e);
            return;
        }
    };
    let buffer_result = record_batch_to_buffer(batches);
    let buffer = match buffer_result {
        Ok(buf) => buf,
        Err(e) => {
            println!("Failed to convert record batch to buffer, ({:?})", e);
            return;
        }
    };
    println!("buffer size: {}", buffer.len());
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

}
