use std::sync::Arc;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{io::Cursor, time::Instant};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use polars::prelude::{
    CsvReader, CsvWriter, DataFrame, QuoteStyle, SerReader, SerWriter, SortOptions,
};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;

extern crate serde_json;

const DEFAULT_CONCURRENCY_LIMIT: usize = 1_000;

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    part_number: u32,
    etag: String,
    init_fn_reduce: String,
    post_download_reduce: String,
    pre_upload_reduce: String,
    end_fn_reduce: String,
}

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

async fn sort_reduce(args: Input) -> Output {
    let init_fn_reduce = get_timestamp_in_milliseconds().unwrap().to_string();

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
                .build()
        }
        None => aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials_provider)
            .region(Region::new(args.s3_config.region.clone()))
            .build(),
    };
    let s3_client = S3Client::from_conf(config);

    let semaphore = Arc::new(Semaphore::new(DEFAULT_CONCURRENCY_LIMIT));

    let mut requests = (0..args.partitions)
        .map(|i| {
            let key = format!(
                "{}{}/{}.part{}",
                args.tmp_prefix, args.partition_idx, args.key, i
            );
            tokio::spawn({
                let client = s3_client.clone();
                let bucket = args.bucket.clone();
                let semaphore = semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    client.get_object().bucket(bucket).key(key).send().await
                }
            })
        })
        .collect::<FuturesUnordered<_>>();

    let mut results = Vec::with_capacity(args.partitions as usize);

    while let Some(f) = requests.next().await {
        match f {
            Ok(f) => match f {
                Ok(res) => {
                    results.push(res);
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            },
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }

    let mut agg_df: Option<DataFrame> = None;
    for res in results {
        let mut buffer: Vec<u8> = Vec::with_capacity(res.content_length.unwrap() as usize);
        let mut reader = res.body.into_async_read();
        while let Ok(sz) = reader.read_buf(&mut buffer).await {
            if sz == 0 {
                break;
            }
        }

        let cursor = Cursor::new(buffer);

        let df_chunk: DataFrame = CsvReader::new(cursor)
            .infer_schema(Some(1))
            .has_header(false)
            .with_quote_char(None)
            .finish()
            .unwrap();

        let a = match agg_df {
            Some(ref df) => df.vstack(&df_chunk).unwrap(),
            None => df_chunk,
        };
        agg_df = Some(a);
    }

    let post_download_reduce = get_timestamp_in_milliseconds().unwrap().to_string();

    let mut agg_df = agg_df.unwrap();
    let agg_df = agg_df.align_chunks();
    println!("Bucket {} has {} rows", args.partition_idx, agg_df.height());

    println!("{:?}", agg_df.get_column_names()[0]);

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
    println!("Sort time: {:?}", sort_duration);

    let pre_upload_reduce = get_timestamp_in_milliseconds().unwrap().to_string();

    let mut buf = Vec::new();
    let write_start_t = Instant::now();
    CsvWriter::new(&mut buf)
        .has_header(false)
        .with_quote_style(QuoteStyle::Never)
        .finish(&mut agg_df)
        .unwrap();
    let write_duration = write_start_t.elapsed();
    println!("Serialize time: {:?}", write_duration);

    let len = buf.len();

    let req = s3_client
        .upload_part()
        .bucket(args.bucket.clone())
        .key(args.mpu_id.clone())
        .part_number((args.partition_idx + 1) as i32)
        .upload_id(args.mpu_id.clone())
        .body(buf.into());

    println!("Uploading part {:?}, size: {:?}", args.partition_idx, len);

    let part_upload_res = req.send().await.unwrap();

    let end_fn_reduce = get_timestamp_in_milliseconds().unwrap().to_string();

    let etag = part_upload_res.e_tag.unwrap();
    println!("etag: {}", etag);
    Output {
        bucket: args.bucket,
        key: args.mpu_key,
        part_number: args.partition_idx,
        etag,
        init_fn_reduce,
        post_download_reduce,
        pre_upload_reduce,
        end_fn_reduce,
    }
}

pub fn main(args: Value) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;

    // create tokio thread runtime
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = tokio_runtime.block_on(async { sort_reduce(input).await });

    serde_json::to_value(result)
}

// main function acts as a wrapper of what the OW runtime would do
// fn main() {
//     // get input from file, this would be the payload from invokation request of OW
//     let file = File::open("../sort_payload.json").unwrap();

//     // parse JSON into array of Input structs
//     let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();

//     for input in inputs {
//         match ow_main(serde_json::to_value(input).unwrap()) {
//             Ok(output) => println!("{}", output),
//             Err(e) => println!("{}", e),
//         }
//     }
// }
