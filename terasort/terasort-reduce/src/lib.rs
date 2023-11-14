use std::{fs::File, io::Cursor, time::Instant};

use polars::prelude::{
    CsvReader, CsvWriter, DataFrame, QuoteStyle, SerReader, SerWriter, SortOptions,
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
    obj_size: u32,
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
    rabbitmq_config: RabbitMQConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3Config {
    region: String,
    endpoint: String,
    aws_access_key_id: String,
    aws_secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RabbitMQConfig {
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    part_number: u32,
    etag: String,
}

async fn sort_reduce(args: Input) -> Output {
    // using abandoned rusoto lib because aws sdk beta sucks and does not work with minio
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: args.s3_config.region,
        endpoint: args.s3_config.endpoint,
    };
    let creds = rusoto_core::credential::StaticProvider::new_minimal(
        args.s3_config.aws_access_key_id,
        args.s3_config.aws_secret_access_key,
    );
    let s3_client = S3Client::new_with(client, creds, region);

    let mut agg_df: Option<DataFrame> = None;

    for i in 0..args.partitions {
        let key = format!(
            "{}{}/{}.part{}",
            args.tmp_prefix, args.partition_idx, args.key, i
        );
        let get_req = GetObjectRequest {
            bucket: args.bucket.clone(),
            key: key,
            ..Default::default()
        };
        println!("{:?}", get_req);
        let get_res = s3_client.get_object(get_req).await.unwrap();

        let mut buffer: Vec<u8> = Vec::with_capacity(get_res.content_length.unwrap() as usize);
        let mut reader = get_res.body.unwrap().into_async_read();
        while let Ok(sz) = reader.read_buf(&mut buffer).await {
            if sz == 0 {
                break;
            }
        }

        let cursor = Cursor::new(buffer);

        let mut df_chunk: DataFrame = CsvReader::new(cursor)
            .infer_schema(Some(1))
            .has_header(false)
            .with_quote_char(None)
            .finish()
            .unwrap();

        let a = match agg_df {
            Some(ref df) => {
                let new = df.vstack(&mut df_chunk).unwrap();
                new
            }
            None => df_chunk,
        };
        agg_df = Some(a);
    }

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

    let mut buf = Vec::new();
    let write_start_t = Instant::now();
    CsvWriter::new(&mut buf)
        .has_header(false)
        .with_quote_style(QuoteStyle::Never)
        .finish(&mut agg_df)
        .unwrap();
    let write_duration = write_start_t.elapsed();
    println!("Serialize time: {:?}", write_duration);

    let mpu_input = rusoto_s3::UploadPartRequest {
        bucket: args.bucket.clone(),
        key: args.mpu_key.clone(),
        part_number: (args.partition_idx + 1) as i64,
        upload_id: args.mpu_id,
        body: Some(buf.into()),
        ..Default::default()
    };
    println!("{:?}", mpu_input);
    let part_upload_res = s3_client.upload_part(mpu_input).await.unwrap();

    let etag = part_upload_res.e_tag.unwrap();
    println!("etag: {}", etag);
    Output {
        bucket: args.bucket,
        key: args.mpu_key,
        part_number: args.partition_idx,
        etag: etag,
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
