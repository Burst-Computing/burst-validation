use futures::sync::Mutex;
use std::{
    ops::Range,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use burst_communication_middleware::{create_group_handlers, BurstMiddleware, MiddlewareArguments};
use bytes::Bytes;
use clap::Parser;
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
    #[arg(long = "burst-id", required = false, default_value = "shuffle")]
    pub burst_id: String,

    /// Burst Size
    #[arg(long = "burst-size", required = true)]
    pub burst_size: u32,

    /// Groups
    #[arg(long = "groups", required = true)]
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
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .unwrap();

    let args = Arguments::parse();
    println!("{:?}", args);

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
}

async fn worker(burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> Result<()> {
    println!("worker start: id={}", burst_middleware.worker_id);
    let data = Bytes::from(vec![b'x'; payload]);

    let burst_size = burst_middleware.burst_size;
    let worker_id = burst_middleware.worker_id;
    let bm_send = Arc::new(Mutex::new(burst_middleware));
    let bm_receive = bm_send.clone();

    let send = tokio::spawn(async move {
        for _ in 0..repeat {
            for target in 0..burst_size {
                if target == worker_id {
                    continue;
                }
                let bm = bm_send.lock().await;
                if let Err(e) = bm.send(target, data.clone()).await {
                    error!("Error: {}", e);
                }
                drop(bm);
            }
        }
    });

    let receive = tokio::spawn(async move {
        for _ in 0..repeat {
            for target in 0..burst_size {
                if target == worker_id {
                    continue;
                }
                let mut bm = bm_receive.lock().await;
                if let Err(e) = bm.recv().await {
                    error!("Error: {}", e);
                }
                drop(bm);
            }
        }
    });

    let _ = tokio::join!(send, receive);

    println!("worker end: id={}", worker_id);

    Ok(())
}
