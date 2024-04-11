use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{collections::HashMap, io::Cursor, time::Instant};

use aws_config::retry::RetryConfig;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use polars::prelude::{
    AnyValue, ChunkedArray, CsvReader, CsvWriter, DataFrame, QuoteStyle, SerReader, SerWriter,
};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::io::AsyncReadExt;

extern crate serde_json;

use crate::token_bucket::TokenBucket;

mod token_bucket;

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
    endpoint: Option<String>,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
    max_concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    partition_idx: u32,
    reduce_keys: Vec<String>,
    init_fn_map: String,
    post_download_map: String,
    pre_upload_map: String,
    end_fn_map: String,
}

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

async fn sort_map(args: Input) -> Output {
    let init_fn_map = get_timestamp_in_milliseconds().unwrap().to_string();

    // create s3 client
    let credentials_provider = Credentials::from_keys(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
        args.s3_config.aws_session_token.clone(),
    );

    let config = match args.s3_config.endpoint.clone() {
        Some(endpoint) => {
            aws_sdk_s3::config::Builder::new()
                .endpoint_url(endpoint)
                .credentials_provider(credentials_provider)
                .region(Region::new(args.s3_config.region.clone()))
                .force_path_style(true) // apply bucketname as path param instead of pre-domain
                .retry_config(RetryConfig::adaptive())
                .build()
        }
        None => aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials_provider)
            .region(Region::new(args.s3_config.region.clone()))
            .retry_config(RetryConfig::adaptive())
            .build(),
    };
    let s3_client = S3Client::from_conf(config);

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
    // let byte_range = (
    //     (args.row_size - (byte_range.0 % args.row_size)) + byte_range.0,
    //     (args.row_size - (byte_range.1 % args.row_size)) + byte_range.1,
    // );
    // println!("Byte range: {:?}", byte_range);

    let get_t0 = Instant::now();
    let get_res = s3_client
        .get_object()
        .bucket(args.bucket.clone())
        .key(args.key.clone())
        .range(format!("bytes={}-{}", byte_range.0, byte_range.1))
        .send()
        .await
        .unwrap();

    let mut buffer: Vec<u8> = Vec::with_capacity(((byte_range.1 - byte_range.0) + 1) as usize);
    let mut reader = get_res.body.into_async_read();
    while let Ok(sz) = reader.read_buf(&mut buffer).await {
        if sz == 0 {
            break;
        }
    }

    let post_download_map = get_timestamp_in_milliseconds().unwrap().to_string();
    let get_duration = get_t0.elapsed();
    println!("Get time: {:?}", get_duration);
    println!("Buffer size: {}", buffer.len());

    let cursor = Cursor::new(buffer);
    let df_chunk: DataFrame = CsvReader::new(cursor)
        .infer_schema(Some(1))
        .has_header(false)
        .with_quote_char(None)
        .finish()
        .unwrap();

    // save index in a hashmap
    let mut indexes: HashMap<u32, Vec<u32>> = HashMap::new();

    let shuffle_t0 = Instant::now();
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
    let shuffle_duration = shuffle_t0.elapsed();
    println!("Shuffle time: {:?}", shuffle_duration);

    // println!("Partitions: {:?}", indexes.keys());
    let mut keys: Vec<String> = Vec::with_capacity(indexes.len());
    let pre_upload_map = get_timestamp_in_milliseconds().unwrap().to_string();

    let token_bucket = Arc::new(TokenBucket::new(
        Duration::from_secs(1),
        args.s3_config.max_concurrency,
    ));

    let mut requests = indexes
        .into_iter()
        .map(|(bucket, indexes)| {
            let a = ChunkedArray::from_vec("partition", indexes.clone());
            let mut partition_df = df_chunk.take(&a).unwrap();

            let mut buf = Vec::with_capacity(partition_df.height() * args.row_size as usize);
            let write_start_t = Instant::now();
            CsvWriter::new(&mut buf)
                .has_header(false)
                .with_quote_style(QuoteStyle::Never)
                .finish(&mut partition_df)
                .unwrap();
            let write_duration = write_start_t.elapsed();
            println!("Serialize time: {:?}", write_duration);

            let key = format!(
                "{}{}/{}.part{}",
                args.tmp_prefix, bucket, args.key, args.partition_idx
            );
            keys.push(key.clone());
            println!("Going to upload partition {}, size = {}", key, buf.len());
            tokio::spawn({
                let client = s3_client.clone();
                let bucket = args.bucket.clone();
                let token_bucket = token_bucket.clone();
                async move {
                    token_bucket.acquire().await;
                    (
                        key.clone(),
                        client
                            .put_object()
                            .bucket(bucket)
                            .key(key)
                            .body(buf.into())
                            .send()
                            .await,
                    )
                }
            })
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(f) = requests.next().await {
        match f {
            Ok((k, f)) => match f {
                Ok(_) => {
                    println!("Uploaded partition {:?}", k);
                }
                Err(e) => {
                    println!("Error uploading partition {:?}: {:?}", k, e);
                }
            },
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }

    let end_fn_map = get_timestamp_in_milliseconds().unwrap().to_string();

    Output {
        partition_idx: args.partition_idx,
        reduce_keys: keys,
        init_fn_map,
        post_download_map,
        pre_upload_map,
        end_fn_map,
    }
}

pub fn main(args: Value) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    println!("Starting sort map: {:?}", input);

    // create tokio thread runtime
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = tokio_runtime.block_on(async { sort_map(input).await });
    println!("Done: {:?}", result);

    serde_json::to_value(result)
}

// // main function acts as a wrapper of what the OW runtime would do
// fn main() {
//     // get input from file, this would be the payload from invokation request of OW
//     let file = File::open("sort_payload.json").unwrap();

//     // parse JSON into array of Input structs
//     let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();

//     for input in inputs {
//         match ow_main(serde_json::to_value(input).unwrap()) {
//             Ok(output) => println!("{}", output),
//             Err(e) => println!("{}", e),
//         }
//     }
// }
