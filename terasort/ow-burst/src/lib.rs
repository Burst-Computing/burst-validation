use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{collections::HashMap, io::Cursor, time::Instant};

use burst_communication_middleware::Middleware;
use bytes::Bytes;
use polars::{
    chunked_array::{ops::SortOptions, ChunkedArray},
    datatypes::AnyValue,
    frame::DataFrame,
    io::{
        csv::{CsvReader, CsvWriter, QuoteStyle},
        SerReader, SerWriter,
    },
};
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::io::AsyncReadExt;

extern crate serde_json;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    bucket: String,
    key: String,
    obj_size: u64,
    sort_column: u32,
    delimiter: char,
    partitions: u32,
    partition_idx: u32,
    segment_bounds: Vec<String>,
    row_size: u32,
    mpu_key: String,
    mpu_id: String,
    tmp_prefix: String,
    s3_config: S3Config,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3Config {
    region: String,
    endpoint: String,
    aws_access_key_id: String,
    aws_secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    part_number: u32,
    etag: String,
    init_fn: String,
    post_download: String,
    pre_shuffle: String,
    post_shuffle: String,
    pre_upload: String,
    end_fn: String,
}

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

async fn get_chunk(s3_client: &S3Client, args: &Input) -> Vec<u8> {
    // TODO: last chunk is very small than others.
    // Implement +-1 chunking to make it more balanced and avoid failures in MPUs
    let row_size = args.row_size as u64;
    let chunk_size = ((args.obj_size as f64 / args.partitions as f64) / row_size as f64).ceil()
        as u64
        * row_size;

    let start_byte = args.partition_idx as u64 * chunk_size;
    let end_byte = (args.partition_idx + 1) as u64 * chunk_size;

    let end_byte = if end_byte > args.obj_size {
        args.obj_size
    } else {
        end_byte
    };

    let byte_range = (start_byte, end_byte);

    let get_res = s3_client
        .get_object(GetObjectRequest {
            bucket: args.bucket.clone(),
            key: args.key.clone(),
            range: Some(format!("bytes={}-{}", byte_range.0, byte_range.1)),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut buffer: Vec<u8> = Vec::with_capacity(((byte_range.1 - byte_range.0) + 1) as usize);
    let mut reader = get_res.body.unwrap().into_async_read();
    while let Ok(sz) = reader.read_buf(&mut buffer).await {
        if sz == 0 {
            break;
        }
    }
    buffer
}

async fn upload_chunk_result(args: &Input, buf: Vec<u8>) -> String {
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: args.s3_config.region.clone(),
        endpoint: args.s3_config.endpoint.clone(),
    };
    let creds = rusoto_core::credential::StaticProvider::new_minimal(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
    );
    let s3_client = S3Client::new_with(client, creds, region);
    let mpu_input = rusoto_s3::UploadPartRequest {
        bucket: args.bucket.clone(),
        key: args.mpu_key.clone(),
        part_number: (args.partition_idx + 1) as i64,
        upload_id: args.mpu_id.clone(),
        body: Some(buf.into()),
        ..Default::default()
    };
    // println!("{:?}", mpu_input);
    let part_upload_res = s3_client.upload_part(mpu_input).await.unwrap();
    // println!("{:?}", part_upload_res);
    part_upload_res.e_tag.unwrap()
}

fn sort_burst(args: Input, burst_middleware: Middleware<Bytes>) -> Output {
    let burst_middleware = burst_middleware.get_actor_handle();
    let init_fn = get_timestamp_in_milliseconds().unwrap().to_string();
    // using abandoned rusoto lib because aws sdk beta sucks and does not work with minio
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: args.s3_config.region.clone(),
        endpoint: args.s3_config.endpoint.clone(),
    };
    let creds = rusoto_core::credential::StaticProvider::new_minimal(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
    );
    let s3_client = S3Client::new_with(client, creds, region);

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let get_t0 = Instant::now();
    let chunk = tokio_runtime.block_on(get_chunk(&s3_client, &args));
    let get_duration = get_t0.elapsed();
    let post_download = get_timestamp_in_milliseconds().unwrap().to_string();
    println!(
        "[Worker {}] Get time: {:?}",
        burst_middleware.info.worker_id, get_duration
    );
    println!(
        "[Worker {}] Buffer size: {}",
        burst_middleware.info.worker_id,
        chunk.len()
    );

    // Load the chunk into a DataFrame
    let cursor = Cursor::new(chunk);
    let df_chunk: DataFrame = CsvReader::new(cursor)
        .infer_schema(Some(1))
        .has_header(false)
        .with_quote_char(None)
        .finish()
        .unwrap();

    // Here we calculate using binary search the indexes to which bucket each row should go
    let mut indexes: HashMap<u32, Vec<u32>> = HashMap::new();
    let shuffle_t0 = Instant::now();
    let pre_shuffle = get_timestamp_in_milliseconds().unwrap().to_string();
    for (idx, value) in df_chunk[args.sort_column as usize].iter().enumerate() {
        // println!("{}", x);
        let res = match value {
            AnyValue::Utf8(s) => args.segment_bounds.binary_search(&s.to_string()),
            _ => panic!("Not a string"),
        };
        match res {
            Ok(x) => {
                indexes.entry(x as u32).or_default().push(idx as u32);
            }
            Err(x) => {
                indexes.entry(x as u32).or_default().push(idx as u32);
            }
        };
    }
    for (bucket, idxs) in indexes {
        // Get the rows that belong to this bucket taking the indexes calculated before
        let chunked_array = ChunkedArray::from_vec("partition", idxs);
        let mut partition_df = df_chunk.take(&chunked_array).unwrap();

        // Serialize the partition to CSV and send it to the corresponding worker
        let mut buf = Vec::new();
        let write_start_t = Instant::now();
        CsvWriter::new(&mut buf)
            .has_header(false)
            .with_quote_style(QuoteStyle::Never)
            .finish(&mut partition_df)
            .unwrap();
        let write_duration = write_start_t.elapsed();
        println!(
            "[Worker {}] Serialize time: {:?}",
            burst_middleware.info.worker_id, write_duration
        );

        println!(
            "[Worker {}] Going to send partition to worker {}, size = {}",
            burst_middleware.info.worker_id,
            bucket,
            buf.len()
        );
        let send_t0 = Instant::now();
        burst_middleware.send(bucket, Bytes::from(buf)).unwrap();
        let send_duration = send_t0.elapsed();
        println!(
            "[Worker {}] Send time: {:?}",
            burst_middleware.info.worker_id, send_duration
        );
    }

    let mut agg_df: Option<DataFrame> = None;
    for i in 0..args.partitions {
        let msg = burst_middleware.recv(i).unwrap();
        println!(
            "[Worker {}] Received partition {} from worker {}",
            burst_middleware.info.worker_id, i, i
        );
        let cursor = Cursor::new(msg);

        let df_chunk: DataFrame = CsvReader::new(cursor)
            .infer_schema(Some(1))
            .has_header(false)
            .with_quote_char(None)
            .finish()
            .unwrap();

        let df = match agg_df {
            Some(ref df) => df.vstack(&df_chunk).unwrap(),
            None => df_chunk,
        };
        agg_df = Some(df);
    }
    let post_shuffle = get_timestamp_in_milliseconds().unwrap().to_string();
    let shuffle_duration = shuffle_t0.elapsed();
    println!(
        "[Worker {}] Shuffle time: {:?}",
        burst_middleware.info.worker_id, shuffle_duration
    );

    let mut agg_df = agg_df.unwrap();
    let agg_df = agg_df.align_chunks();
    println!(
        "[Worker {}] Bucket {} has {} rows",
        burst_middleware.info.worker_id,
        args.partition_idx,
        agg_df.height()
    );

    // println!("{:?}", agg_df.get_column_names()[0]);

    let sort_options = SortOptions {
        descending: false,
        nulls_last: true,
        multithreaded: false,
        maintain_order: true,
    };

    let sort_start_t = Instant::now();
    let mut agg_df = agg_df
        .sort_with_options(
            agg_df.get_column_names()[args.sort_column as usize],
            sort_options,
        )
        .unwrap();
    let sort_duration = sort_start_t.elapsed();
    println!(
        "[Worker {}] Sort time: {:?}",
        burst_middleware.info.worker_id, sort_duration
    );

    let mut buf = Vec::new();
    let write_start_t = Instant::now();
    CsvWriter::new(&mut buf)
        .has_header(false)
        .with_quote_style(QuoteStyle::Never)
        .finish(&mut agg_df)
        .unwrap();
    let write_duration = write_start_t.elapsed();
    println!(
        "[Worker {}] Serialize time: {:?}",
        burst_middleware.info.worker_id, write_duration
    );
    let pre_upload = get_timestamp_in_milliseconds().unwrap().to_string();
    let put_part_t0 = Instant::now();
    let etag = tokio_runtime.block_on(upload_chunk_result(&args, buf));
    let put_part_duration = put_part_t0.elapsed();
    println!(
        "[Worker {}] Put part time: {:?}",
        burst_middleware.info.worker_id, put_part_duration
    );

    let end_fn = get_timestamp_in_milliseconds().unwrap().to_string();
    // println!("etag: {}", etag);
    Output {
        bucket: args.bucket,
        key: args.mpu_key,
        part_number: args.partition_idx,
        etag,
        init_fn,
        post_download,
        pre_shuffle,
        post_shuffle,
        pre_upload,
        end_fn,
    }
}

fn sort_burst_all2all(args: Input, burst_middleware: Middleware<Bytes>) -> Output {
    let burst_middleware = burst_middleware.get_actor_handle();
    let init_fn = get_timestamp_in_milliseconds().unwrap().to_string();
    // using abandoned rusoto lib because aws sdk beta sucks and does not work with minio
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: args.s3_config.region.clone(),
        endpoint: args.s3_config.endpoint.clone(),
    };
    let creds = rusoto_core::credential::StaticProvider::new_minimal(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
    );
    let s3_client = S3Client::new_with(client, creds, region);

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let get_t0 = Instant::now();
    let chunk = tokio_runtime.block_on(get_chunk(&s3_client, &args));
    let get_duration = get_t0.elapsed();
    let post_download = get_timestamp_in_milliseconds().unwrap().to_string();
    println!(
        "[Worker {}] Get time: {:?}",
        burst_middleware.info.worker_id, get_duration
    );
    println!(
        "[Worker {}] Buffer size: {}",
        burst_middleware.info.worker_id,
        chunk.len()
    );

    // Load the chunk into a DataFrame
    let cursor = Cursor::new(chunk);
    let df_chunk: DataFrame = CsvReader::new(cursor)
        .infer_schema(Some(1))
        .has_header(false)
        .with_quote_char(None)
        .finish()
        .unwrap();

    // Here we calculate using binary search the indexes to which bucket each row should go
    let mut indexes: HashMap<u32, Vec<u32>> = HashMap::new();
    let shuffle_t0 = Instant::now();
    let pre_shuffle = get_timestamp_in_milliseconds().unwrap().to_string();
    for (idx, value) in df_chunk[args.sort_column as usize].iter().enumerate() {
        // println!("{}", x);
        let res = match value {
            AnyValue::Utf8(s) => args.segment_bounds.binary_search(&s.to_string()),
            _ => panic!("Not a string"),
        };
        match res {
            Ok(x) => {
                indexes.entry(x as u32).or_default().push(idx as u32);
            }
            Err(x) => {
                indexes.entry(x as u32).or_default().push(idx as u32);
            }
        };
    }
    let mut exchange_vec = vec![Bytes::new(); burst_middleware.info.burst_size as usize];
    for (bucket, idxs) in indexes {
        // Get the rows that belong to this bucket taking the indexes calculated before
        let chunked_array = ChunkedArray::from_vec("partition", idxs);
        let mut partition_df = df_chunk.take(&chunked_array).unwrap();

        // Serialize the partition to CSV and send it to the corresponding worker
        let mut buf = Vec::new();
        let write_start_t = Instant::now();
        CsvWriter::new(&mut buf)
            .has_header(false)
            .with_quote_style(QuoteStyle::Never)
            .finish(&mut partition_df)
            .unwrap();
        let write_duration = write_start_t.elapsed();
        println!(
            "[Worker {}] Serialize time: {:?}",
            burst_middleware.info.worker_id, write_duration
        );

        println!(
            "[Worker {}] Has to send partition to worker {}, size = {}",
            burst_middleware.info.worker_id,
            bucket,
            buf.len()
        );
        exchange_vec[bucket as usize] = Bytes::from(buf);
    }

    let t0 = Instant::now();
    let exchanged_vec = burst_middleware
        .all_to_all(exchange_vec)
        .expect("Could not execute all-to-all");
    let duration = t0.elapsed();

    println!(
        "[Worker {}] All-to-all time: {:?}",
        burst_middleware.info.worker_id, duration
    );

    let mut agg_df = exchanged_vec
        .into_iter()
        .map(|x| {
            let cursor = Cursor::new(x);
            CsvReader::new(cursor)
                .infer_schema(Some(1))
                .has_header(false)
                .with_quote_char(None)
                .finish()
                .unwrap()
        })
        .fold(DataFrame::empty(), |acc, df| acc.vstack(&df).unwrap());
    let post_shuffle = get_timestamp_in_milliseconds().unwrap().to_string();
    let shuffle_duration = shuffle_t0.elapsed();
    println!(
        "[Worker {}] Shuffle time: {:?}",
        burst_middleware.info.worker_id, shuffle_duration
    );

    let agg_df = agg_df.align_chunks();
    println!(
        "[Worker {}] Bucket {} has {} rows",
        burst_middleware.info.worker_id,
        args.partition_idx,
        agg_df.height()
    );

    // println!("{:?}", agg_df.get_column_names()[0]);

    let sort_options = SortOptions {
        descending: false,
        nulls_last: true,
        multithreaded: false,
        maintain_order: true,
    };

    let sort_start_t = Instant::now();
    let mut agg_df = agg_df
        .sort_with_options(
            agg_df.get_column_names()[args.sort_column as usize],
            sort_options,
        )
        .unwrap();
    let sort_duration = sort_start_t.elapsed();
    println!(
        "[Worker {}] Sort time: {:?}",
        burst_middleware.info.worker_id, sort_duration
    );

    let mut buf = Vec::new();
    let write_start_t = Instant::now();
    CsvWriter::new(&mut buf)
        .has_header(false)
        .with_quote_style(QuoteStyle::Never)
        .finish(&mut agg_df)
        .unwrap();
    let write_duration = write_start_t.elapsed();
    println!(
        "[Worker {}] Serialize time: {:?}",
        burst_middleware.info.worker_id, write_duration
    );
    let pre_upload = get_timestamp_in_milliseconds().unwrap().to_string();
    let put_part_t0 = Instant::now();
    let etag = tokio_runtime.block_on(upload_chunk_result(&args, buf));
    let put_part_duration = put_part_t0.elapsed();
    println!(
        "[Worker {}] Put part time: {:?}",
        burst_middleware.info.worker_id, put_part_duration
    );

    let end_fn = get_timestamp_in_milliseconds().unwrap().to_string();
    // println!("etag: {}", etag);
    Output {
        bucket: args.bucket,
        key: args.mpu_key,
        part_number: args.partition_idx,
        etag,
        init_fn,
        post_download,
        pre_shuffle,
        post_shuffle,
        pre_upload,
        end_fn,
    }
}

// ow_main would be the entry point of an actual open whisk burst worker
pub fn main(args: Value, burst_middleware: Middleware<Bytes>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    println!("Starting sort: {:?}", input);

    //let result = sort_burst(input, burst_middleware);
    let result = sort_burst_all2all(input, burst_middleware);

    println!("Done");
    println!("{:?}", result);
    serde_json::to_value(result)
}

// main function acts as a wrapper of what the OW runtime would do, used for debugging
// #[tokio::main]
// async fn main() {
//     let file = File::open("sort_payload.json").unwrap();
//     let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();

//     let burst_size = 4;
//     let mut group_ranges: HashMap<String, HashSet<u32>> = HashMap::new();
//     let range = (0..burst_size).collect::<HashSet<u32>>();
//     group_ranges.insert("0".to_string(), range);

//     let proxies = match BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
//         BurstOptions::new(
//             "terasort".to_string(),
//             burst_size,
//             group_ranges,
//             0.to_string(),
//         ),
//         TokioChannelOptions::new()
//             .broadcast_channel_size(256)
//             .build(),
//         RabbitMQOptions::new(inputs[0].rabbitmq_config.uri.clone())
//             .durable_queues(true)
//             .ack(true)
//             .build(),
//     )
//     .await
//     {
//         Ok(p) => p,
//         Err(e) => {
//             // error!("{:?}", e);
//             println!("{:?}", e);
//             panic!();
//         }
//     };

//     let mut threads = Vec::with_capacity(inputs.len());
//     for (proxy, input) in zip(proxies, inputs) {
//         let (idx, middleware) = proxy;
//         let thread = thread::spawn(move || {
//             println!("thread start: id={}", idx);
//             let result = ow_main(serde_json::to_value(input).unwrap(), middleware).unwrap();
//             println!("thread end: id={}", idx);
//             result
//         });
//         threads.push(thread);
//     }

//     for (i, t) in threads.into_iter().enumerate() {
//         let result = t.join().unwrap();
//         // write output to file, this would be the response of OW invokation
//         let file = File::create(format!("output_{}.json", i)).unwrap();
//         serde_json::to_writer(file, &result).unwrap();
//     }
// }
