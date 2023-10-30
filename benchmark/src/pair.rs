use std::{
    collections::{HashMap, HashSet},
    thread,
    time::{Instant, SystemTime},
};

use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMImpl, RabbitMQOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Debug)]
pub struct Arguments {
    /// RabbitMQ server address
    #[arg(long = "rabbitmq-server", required = true)]
    pub rabbitmq_server: String,

    /// Burst ID
    #[arg(long = "burst-id", required = false, default_value = "pair")]
    pub burst_id: String,

    /// Burst Size
    #[arg(long = "burst-size", required = true)]
    pub burst_size: u32,

    /// Groups
    #[arg(long = "groups", required = false, default_value = "2")]
    pub groups: u32,

    /// Group id
    #[arg(long = "group-id", required = true)]
    pub group_id: String,

    /// Payload size
    #[arg(long = "payload-size", required = false, default_value = "1048576")] // 1MB
    pub payload_size: usize,

    /// Repeat count
    #[arg(long = "repeat", required = false, default_value = "256")] // 256MB
    pub repeat: u32,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    
    let args = Arguments::parse();
    info!("{:?}", args);

    if (args.burst_size % 2) != 0 {
        panic!("Burst size must be even number");
    }

    let data_per_worker = args.payload_size * args.repeat as usize;
    let total_data = data_per_worker * (args.burst_size / 2) as usize;
    info!(
        "Running pair benchmark... Total data to transmit: {} MB ({} MB per worker)",
        total_data / 1024 / 1024,
        data_per_worker / 1024 / 1024
    );

    let group_size = args.burst_size / args.groups;

    let group_ranges = (0..args.groups)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((group_size * group_id)..((group_size * group_id) + group_size)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let burst_options =
        BurstOptions::new(args.burst_id, args.burst_size, group_ranges, args.group_id);

    let channel_options = TokioChannelOptions::new()
        .broadcast_channel_size(256)
        .build();

    let rabbitmq_options = RabbitMQOptions::new(args.rabbitmq_server)
        .durable_queues(true)
        .ack(true)
        .build();

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

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("start: {}", t.as_millis() as f64 / 1000.0);

    let mut threads = Vec::with_capacity(proxies.len());
    for (worker_id, proxy) in proxies {
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = tokio_runtime
                .block_on(async { worker(proxy, args.payload_size, args.repeat).await.unwrap() });
            info!("thread end: id={}", worker_id);
            result
        });
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("end: {}", t.as_millis() as f64 / 1000.0);
}

async fn worker(burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> Result<()> {
    info!("worker {} start", burst_middleware.info().worker_id);

    if burst_middleware.info().worker_id < (burst_middleware.info().burst_size / 2) {
        let t0 = Instant::now();
        let mut total_size = 0;
        for _ in 0..repeat {
            let msg = burst_middleware.recv().await?;
            debug!(
                "Worker {} Received from {}",
                burst_middleware.info().worker_id,
                msg.sender_id
            );
            total_size += msg.data.len();
        }
        let t = t0.elapsed();
        info!(
            "Worker {} - received {} messages in {} s",
            burst_middleware.info().worker_id,
            repeat,
            t.as_millis() as f64 / 1000.0,
        );
    } else {
        let data = Bytes::from(vec![b'x'; payload]);
        let target = burst_middleware.info().worker_id % (burst_middleware.info().burst_size / 2);
        debug!(
            "Worker {} Sending to {}",
            burst_middleware.info().worker_id,
            target
        );
        let t0 = Instant::now();
        for _ in 0..repeat {
            burst_middleware.send(target, data.clone()).await?;
        }
        let t = t0.elapsed();
        info!(
            "Worker {} - sent {} messages in {} s",
            burst_middleware.info().worker_id,
            repeat,
            t.as_millis() as f64 / 1000.0,
        );
    }

    info!("worker {} end", burst_middleware.info().worker_id);

    Ok(())
}
