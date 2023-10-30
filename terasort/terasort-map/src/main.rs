use std::{collections::HashMap, fs::File, io::Cursor, time::Instant};

use polars::prelude::{
    AnyValue, ChunkedArray, CsvReader, CsvWriter, DataFrame, QuoteStyle, SerReader, SerWriter,
};
use rusoto_core::Region;
use rusoto_credential::EnvironmentProvider;
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
    tmp_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    name: String,
}

async fn sort_map(args: Input) {
    // using abandoned rusoto lib because aws sdk beta sucks and does not work with minio
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: "us-east-1".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
    };
    let creds = EnvironmentProvider::default();
    let s3_client = S3Client::new_with(client, creds, region);

    let chunk_size = (args.obj_size as f64 / args.partitions as f64).ceil() as u32;

    let byte_range = (
        args.partition_idx * chunk_size,
        (args.partition_idx * chunk_size) + chunk_size,
    );
    // let byte_range = (
    //     (args.row_size - (byte_range.0 % args.row_size)) + byte_range.0,
    //     (args.row_size - (byte_range.1 % args.row_size)) + byte_range.1,
    // );
    // println!("Byte range: {:?}", byte_range);

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

    let cursor = Cursor::new(buffer);

    let df_chunk: DataFrame = CsvReader::new(cursor)
        .infer_schema(Some(1))
        .has_header(false)
        .with_quote_char(None)
        .finish()
        .unwrap();

    // save index in a hashmap
    let mut indexes: HashMap<u32, Vec<u32>> = HashMap::new();

    for (idx, value) in df_chunk[args.sort_column as usize].iter().enumerate() {
        // println!("{}", x);
        let res = match value {
            AnyValue::Utf8(s) => args.segment_bounds.binary_search(&s.to_string()),
            _ => panic!("Not a string"),
        };
        match res {
            Ok(x) => {
                indexes
                    .entry(x as u32)
                    .or_insert(Vec::new())
                    .push(idx as u32);
            }
            Err(x) => {
                indexes
                    .entry(x as u32)
                    .or_insert(Vec::new())
                    .push(idx as u32);
            }
        };
    }

    // println!("Partitions: {:?}", indexes.keys());
    // let mut tasks = vec![];
    for (bucket, indexes) in indexes.iter() {
        let a = ChunkedArray::from_vec("partition", indexes.clone());
        let mut partition_df = df_chunk.take(&a).unwrap();

        let mut buf = Vec::new();
        let write_start_t = Instant::now();
        CsvWriter::new(&mut buf)
            .has_header(false)
            .with_quote_style(QuoteStyle::Never)
            .finish(&mut partition_df)
            .unwrap();
        let write_duration = write_start_t.elapsed();
        // println!("Serialize time: {:?}", write_duration);

        let put_object_request = rusoto_s3::PutObjectRequest {
            bucket: args.bucket.clone(),
            key: format!(
                "{}{}/{}.part{}",
                args.tmp_prefix, bucket, args.key, args.partition_idx
            ),
            body: Some(buf.into()),
            ..Default::default()
        };
        // let task = s3_client.put_object(put_object_request);
        // tasks.push(task);
        s3_client.put_object(put_object_request).await.unwrap();
    }

    // futures::future::join_all(tasks).await;
}

pub fn ow_main(args: Value) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;

    // create tokio thread runtime
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = tokio_runtime.block_on(async {
        sort_map(input).await;
    });

    let output = Output {
        name: format!("Hello"),
    };
    serde_json::to_value(output)
}

// main function acts as a wrapper of what the OW runtime would do
fn main() {
    // get input from file, this would be the payload from invokation request of OW
    let file = File::open("sort_payload.json").unwrap();

    // parse JSON into array of Input structs
    let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();

    for input in inputs {
        match ow_main(serde_json::to_value(input).unwrap()) {
            Ok(output) => println!("{}", output),
            Err(e) => println!("{}", e),
        }
    }
}
