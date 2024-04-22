use aws_config::Region;
use aws_config::retry::RetryConfig;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::Middleware;
use bytes::{Bytes};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    bucket: String,
    key: String,
    s3_config: S3Config,
    start_byte: i64,
    end_byte: i64,
    base_worker_id: u32,
    granularity: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3Config {
    region: String,
    endpoint: Option<String>,
    aws_access_key_id: String,
    aws_secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    success: bool,
    init_fn: String,
    post_download_chunk: String,
    input_gathered: String,
    end_fn: String,
}

fn get_timestamp_in_milliseconds() -> Result<u128, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
    let milliseconds_timestamp = duration_since_epoch.as_millis();

    Ok(milliseconds_timestamp)
}

async fn get_chunk(s3_client: &S3Client, args: &Input) -> Vec<u8> {
    let byte_range = (args.start_byte, args.end_byte);

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
    buffer
}


fn hyperparameter_tuning(args: Input,  burst_middleware: Middleware<Bytes>) -> Option<Output> {
    let burst_middleware = burst_middleware.get_actor_handle();
    let init_fn = get_timestamp_in_milliseconds().unwrap().to_string();

    let credentials_provider = Credentials::from_keys(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
        None
    );

    let config = match args.s3_config.endpoint.clone() {
        Some(endpoint) => {
            aws_sdk_s3::config::Builder::new()
                .endpoint_url(endpoint)
                .credentials_provider(credentials_provider)
                .region(Region::new(args.s3_config.region.clone()))
                .force_path_style(true)
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

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let buffer = tokio_runtime.block_on(get_chunk(&s3_client, &args));

    let mut messages: Vec<Bytes> = Vec::new();
    if burst_middleware.info.worker_id != args.base_worker_id {
        burst_middleware.send(args.base_worker_id, Bytes::from(buffer));
    } else {
        messages.push(Bytes::from(buffer));
        for i in (args.base_worker_id + 1)..(args.base_worker_id + args.granularity) {
            messages.push(burst_middleware.recv(i).unwrap());
        }
    }
    let mut buffer: Vec<u8> = Vec::new();
    for message in messages {
        buffer.extend_from_slice(&message);
    }

    let post_download_chunk = get_timestamp_in_milliseconds().unwrap().to_string();

    let mut file = File::create("input.txt").unwrap();
    file.write_all(&buffer).unwrap();

    let input_gathered = get_timestamp_in_milliseconds().unwrap().to_string();

    if burst_middleware.info.worker_id != args.base_worker_id {
        burst_middleware.send(args.base_worker_id, Bytes::from("done"));
    } else {
        for i in (args.base_worker_id + 1)..(args.base_worker_id + args.granularity) {
            burst_middleware.recv(i);
        }
    }

    let end_fn = get_timestamp_in_milliseconds().unwrap().to_string();

    Some(Output {
        success: true,
        init_fn,
        post_download_chunk,
        input_gathered,
        end_fn,
    })
}

pub fn main(args: Value, burst_middleware: Middleware<Bytes>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    println!("Starting hyperparameter tuning: {:?}", input);

    let result = hyperparameter_tuning(input, burst_middleware);

    println!("Done");
    println!("{:?}", result);
    serde_json::to_value(result)
}
