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
use log::{error, info};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[derive(Parser, Debug)]
pub struct Arguments {
    /// RabbitMQ server address
    #[arg(
        long = "rabbitmq-server",
        default_value = "amqp://guest:guest@localhost:5672",
        required = false
    )]
    pub rabbitmq_server: String,

    /// Burst ID
    #[arg(long = "burst-id", required = false, default_value = "gather")]
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
    // Setup logging

    if let Err(err) = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
    {
        eprintln!("Failed to initialize logging: {}", err);
    }

    let args = Arguments::parse();

    info!("{:?}", args);

    if (args.burst_size % 2) != 0 {
        panic!("Burst size must be even number");
    }

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
                .block_on(async { worker(proxy, args.payload_size, args.repeat).await });
            info!("thread end: id={}", worker_id);
            result
        });
        threads.push(thread);
    }

    let mut agg_throughput: f64 = 0.0;
    for thread in threads {
        let throughput = thread.join().unwrap();
        agg_throughput += throughput;
    }

    info!("Total throughput: {} MB/s", agg_throughput as f64);

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("end: {}", t.as_millis() as f64 / 1000.0);
}

async fn worker(burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> f64 {
    let id = burst_middleware.info().worker_id;
    info!("worker start: id={}", id);

    let data = Bytes::from(vec![b'x'; payload]);

    let throughput;

    // If id 0, receiver
    if id == 0 {
        let mut received_bytes = 0;
        let mut received_messages = 0;
        let t0: Instant = Instant::now();

        info!("Worker {} - started receiving", id);
        for _ in 0..repeat {
            let msgs = burst_middleware
                .gather(data.clone())
                .await
                .unwrap()
                .unwrap();
            // skip the first message (self message)
            received_messages += msgs.len() - 1;
            received_bytes += msgs
                .into_iter()
                .skip(1)
                .fold(0, |acc, msg| acc + msg.data.len());
        }

        let t = t0.elapsed();
        let size_mb = received_bytes as f64 / 1024.0 / 1024.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - received {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            id,
            size_mb,
            received_messages,
            t.as_millis() as f64 / 1000.0,
            t.as_millis() as f64 / 1000.0 / repeat as f64,
            throughput
        );
    // If id != 0, sender
    } else {
        let t0: Instant = Instant::now();

        info!("Thread {} started sending", id);
        for _ in 0..repeat {
            burst_middleware.gather(data.clone()).await.unwrap();
        }

        let t = t0.elapsed();
        let total_size = data.len() * repeat as usize;
        let size_mb = total_size as f64 / 1024.0 / 1024.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - sent {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            t.as_millis() as f64 / repeat as f64,
            throughput
        );
    }

    info!("worker {} end", id);
    throughput
}
