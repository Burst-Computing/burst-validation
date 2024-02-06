use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMImpl, RabbitMQOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use burst_communication_middleware::
    MiddlewareActorHandle
;

use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    thread,
};

use kmeans::{burst_worker, utils::S3Credentials};

const BURST_SIZE: u32 = 1;
const GROUPS: u32 = 1;

#[tokio::main]
async fn main() {
    env_logger::init();

    let s3_credentials = S3Credentials {
        access_key_id: "".to_string(),
        secret_access_key: "".to_string(),
        session_token: Some("".to_string()),
    };

    if BURST_SIZE % GROUPS != 0 {
        panic!("BURST_SIZE must be divisible by GROPUS");
    }

    let group_size = BURST_SIZE / GROUPS;

    let group_ranges = (0..GROUPS)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((group_size * group_id)..((group_size * group_id) + group_size)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let mut threads = Vec::with_capacity(BURST_SIZE as usize);

    for group_id in 0..GROUPS {
        let s3_cred = s3_credentials.clone();

        let burst_options = BurstOptions::new(
            "broadcast".to_string(),
            BURST_SIZE,
            group_ranges.clone(),
            group_id.to_string(),
            true,
            1024*1024
        );

        let channel_options = TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build();

        let rabbitmq_options =
            RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
                .durable_queues(true)
                .ack(true)
                .build();

        let group_threads = group(burst_options, channel_options, rabbitmq_options, s3_cred).await;
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn group(
    burst_options: BurstOptions,
    channel_options: TokioChannelOptions,
    rabbitmq_options: RabbitMQOptions,
    s3_credentials: S3Credentials,
) -> Vec<std::thread::JoinHandle<()>> {
    let proxies = match BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
        burst_options,
        channel_options,
        rabbitmq_options,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("{:?}", e);
            panic!();
        }
    };

    let mut threads = Vec::with_capacity(proxies.len());

    let s3_cred = s3_credentials.clone();
    for (worker_id, proxy) in proxies {
        let s3_cred_copy = s3_cred.clone();
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            let s3_uri = format!("");

            let actor = MiddlewareActorHandle::new(proxy, &tokio_runtime);

            let result = tokio_runtime
                .block_on(async { worker(s3_uri, s3_cred_copy, actor).await.unwrap() });
            info!("thread end: id={}", worker_id);
            result
        });
        threads.push(thread);
    }

    return threads;
}

pub async fn worker(
    s3_uri: String,
    s3_credentials: S3Credentials,
    burst_middleware: MiddlewareActorHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let threshold = 0.00001;
    let num_dimensions = 2;
    let num_clusters = 4;
    let max_iterations = 100;

    burst_worker::burst_worker(
        s3_uri,
        s3_credentials,
        burst_middleware,
        threshold,
        num_dimensions,
        num_clusters,
        max_iterations,
    )
    .await
}