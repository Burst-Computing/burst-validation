use aws_config::retry::RetryConfig;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::Middleware;
use bytes::{Bytes, BytesMut};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use std::fs::File;
use std::io::Write;
use std::time::{Instant, SystemTime, SystemTimeError, UNIX_EPOCH};
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
    mib: Option<f32>,
    python_script: String,
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
    post_send: String,
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

fn hyperparameter_tuning(args: Input, burst_middleware: Middleware<Bytes>) -> Option<Output> {
    let burst_middleware = burst_middleware.get_actor_handle();
    let init_fn = get_timestamp_in_milliseconds().unwrap().to_string();

    let credentials_provider = Credentials::from_keys(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
        None,
    );

    let config = match args.s3_config.endpoint.clone() {
        Some(endpoint) => aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(credentials_provider)
            .region(Region::new(args.s3_config.region.clone()))
            .force_path_style(true)
            .retry_config(RetryConfig::adaptive())
            .build(),
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

    let get_t0 = Instant::now();
    let chunk = tokio_runtime.block_on(get_chunk(&s3_client, &args));
    let get_duration = get_t0.elapsed();

    let post_download_chunk = get_timestamp_in_milliseconds().unwrap().to_string();

    println!(
        "[Worker {}] Downloaded range {}-{} ({} bytes) in {:?}",
        burst_middleware.info.worker_id,
        args.start_byte,
        args.end_byte,
        chunk.len(),
        get_duration,
    );

    let mut buffer = None;
    if burst_middleware.info.worker_id != args.base_worker_id {
        burst_middleware
            .send(args.base_worker_id, Bytes::from(chunk))
            .unwrap();
    } else {
        let mut buff = BytesMut::with_capacity(chunk.len() * args.granularity as usize);
        buff.extend_from_slice(&chunk);
        for i in (args.base_worker_id + 1)..(args.base_worker_id + args.granularity) {
            buff.extend_from_slice(burst_middleware.recv(i).unwrap().as_ref());
        }
        buffer = Some(buff.freeze());
    }

    let post_send = get_timestamp_in_milliseconds().unwrap().to_string();

    let filename = format!(
        "{}/{}-train.ft.txt.bz2",
        std::env::temp_dir().to_str().unwrap(),
        burst_middleware.info.worker_id
    );

    if burst_middleware.info.worker_id == args.base_worker_id {
        let mut file = File::create(&filename).unwrap();
        let len = buffer.as_ref().unwrap().len();
        file.write_all(&buffer.unwrap()).unwrap();

        println!(
            "[Worker {}] Wrote {} bytes to file",
            burst_middleware.info.worker_id, len
        );
    }

    let input_gathered = get_timestamp_in_milliseconds().unwrap().to_string();

    if burst_middleware.info.worker_id == args.base_worker_id {
        // start Python hyperparameter tuning execution
        let mut cmd = std::process::Command::new("python3");
        cmd.arg(&args.python_script);
        cmd.args(["--jobs", args.granularity.to_string().as_str()]);
        cmd.args(["--dataset", &filename]);
        if args.granularity == 1 {
            cmd.envs([
                ("MKL_NUM_THREADS", "1"),
                ("OPENBLAS_NUM_THREADS", "1"),
                ("NUMEXPR_NUM_THREADS", "1"),
            ]);
        }
        if let Some(mib) = args.mib {
            cmd.args(["--mib", &mib.to_string()]);
        }

        println!(
            "[Worker {}] Running command: {:?}",
            burst_middleware.info.worker_id, cmd
        );

        let output = cmd.output().expect("failed to execute process");

        println!(
            "[Worker {}] status: {}",
            burst_middleware.info.worker_id, output.status
        );
        println!(
            "[Worker {}] stdout: {}",
            burst_middleware.info.worker_id,
            String::from_utf8_lossy(&output.stdout)
        );
        println!(
            "[Worker {}] stderr: {}",
            burst_middleware.info.worker_id,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let end_fn = get_timestamp_in_milliseconds().unwrap().to_string();

    Some(Output {
        success: true,
        init_fn,
        post_download_chunk,
        post_send,
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
