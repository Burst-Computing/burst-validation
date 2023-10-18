use std::{
    ops::Range,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime},
};

use burst_communication_middleware::{create_group_handlers, BurstMiddleware, MiddlewareArguments};
use bytes::Bytes;
use clap::{Args, Parser};
use tracing::{error, info};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

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
    pub group_id: u32,

    /// Payload size
    #[arg(long = "payload-size", required = false, default_value = "1048576")] // 1MB
    pub payload_size: usize,

    /// Repeat count
    #[arg(long = "repeat", required = false, default_value = "256")] // 256MB
    pub repeat: u32,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    println!("{:?}", args);

    if (args.burst_size % 2) != 0 {
        panic!("Burst size must be even number");
    }

    let data_per_worker = args.payload_size * args.repeat as usize;
    let total_data = data_per_worker * (args.burst_size / 2) as usize;
    println!(
        "Running pair benchmark... Total data to transmit: {} MB ({} MB per worker)",
        total_data / 1024 / 1024,
        data_per_worker / 1024 / 1024
    );

    let group_size = args.burst_size / args.groups;

    let burst_args = MiddlewareArguments::new(
        args.burst_id,
        args.burst_size,
        args.groups,
        args.group_id,
        (group_size * args.group_id)..((group_size * args.group_id) + group_size),
        args.rabbitmq_server.to_string(),
        true,
        256,
    );

    let handles = create_group_handlers(burst_args).await.unwrap();

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    println!("start: {}", t.as_millis() as f64 / 1000.0);

    let mut threads = Vec::with_capacity(handles.len());
    for handle in handles {
        let thread = thread::spawn(move || {
            let thread_id = handle.worker_id;
            // println!("thread start: id={}", thread_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = tokio_runtime.block_on(async {
                worker(handle, args.payload_size, args.repeat)
                    .await
                    .unwrap()
            });
            // println!("thread end: id={}", thread_id);
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
    println!("end: {}", t.as_millis() as f64 / 1000.0);
}

async fn worker(mut burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> Result<()> {
    // println!("worker {} start", burst_middleware.worker_id);

    if burst_middleware.worker_id < (burst_middleware.burst_size / 2) {
        let t0 = Instant::now();
        let mut total_size = 0;
        for _ in 0..repeat {
            let msg = burst_middleware.recv().await?;
            // println!(
            //     "Worker {} Received from {}",
            //     burst_middleware.worker_id, msg.sender_id
            // );
            total_size += msg.data.len();
        }
        let t = t0.elapsed();
        println!(
            "Worker {} - received {} messages in {} s",
            burst_middleware.worker_id,
            repeat,
            t.as_millis() as f64 / 1000.0,
        );
    } else {
        let data = Bytes::from(vec![b'x'; payload]);
        let target = burst_middleware.worker_id % (burst_middleware.burst_size / 2);
        // println!(
        //     "Worker {} Sending to {}",
        //     burst_middleware.worker_id, target
        // );
        let t0 = Instant::now();
        for _ in 0..repeat {
            burst_middleware.send(target, data.clone()).await?;
        }
        let t = t0.elapsed();
        println!(
            "Worker {} - sent {} messages in {} s",
            burst_middleware.worker_id,
            repeat,
            t.as_millis() as f64 / 1000.0,
        );
    }

    // println!("worker {} end", burst_middleware.worker_id);

    Ok(())
}
