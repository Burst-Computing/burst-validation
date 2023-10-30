use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMImpl, RabbitMQOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use bytes::Bytes;
use clap::Parser;
use std::{
    collections::{HashMap, HashSet},
    thread,
    time::SystemTime,
};
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
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .unwrap();

    let args = Arguments::parse();
    info!("{:?}", args);

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
    info!("worker start: id={}", burst_middleware.info().worker_id);
    let data = Bytes::from(vec![b'x'; payload]);

    for _ in 0..repeat {
        for target in 0..burst_middleware.info().burst_size {
            if target == burst_middleware.info().worker_id {
                continue;
            }
            if let Err(e) = burst_middleware.send(target, data.clone()).await {
                error!("Error: {}", e);
            }
        }
    }

    for _ in 0..repeat {
        for target in 0..burst_middleware.info().burst_size {
            if target == burst_middleware.info().worker_id {
                continue;
            }
            if let Err(e) = burst_middleware.recv().await {
                error!("Error: {}", e);
            }
        }
    }

    info!("worker end: id={}", burst_middleware.info().worker_id);

    Ok(())
}

// async fn worker(burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> Result<()> {
//     info!("worker start: id={}", burst_middleware.worker_id);
//     let data = Bytes::from(vec![b'x'; payload]);

//     let burst_size = burst_middleware.burst_size;
//     let worker_id = burst_middleware.worker_id;
//     let burst_middleware = Arc::new(Mutex::new(burst_middleware));
//     let bm_send = Arc::clone(&burst_middleware);
//     let bm_receive = Arc::clone(&burst_middleware);

//     let send = tokio::spawn(async move {
//         for i in 0..repeat {
//             // info!("send loop {}: id={}", i, worker_id);
//             for target in 0..burst_size {
//                 if target == worker_id {
//                     continue;
//                 } else {
//                     let bm = bm_send.lock().await;
//                     if let Err(e) = bm.send(target, data.clone()).await {
//                         error!("Error: {}", e);
//                     }
//                     drop(bm);
//                 }
//             }
//         }
//         info!("send end: id={}", worker_id);
//     });

//     let receive = tokio::spawn(async move {
//         info!("recieve start: id={}", worker_id);
//         for i in 0..repeat {
//             // info!("receive loop {}: id={}", i, worker_id);
//             for target in 0..burst_size {
//                 if target == worker_id {
//                     continue;
//                 } else {
//                     let mut bm = bm_receive.lock().await;
//                     if let Err(e) = bm.recv().await {
//                         error!("Error: {}", e);
//                     }
//                     drop(bm);
//                 }
//             }
//         }
//         info!("recieve end: id={}", worker_id);
//     });

//     let _ = tokio::join!(send, receive);

//     info!("worker end: id={}", worker_id);

//     Ok(())
// }
