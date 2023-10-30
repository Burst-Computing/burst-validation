use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMImpl, RabbitMQOptions, Result, TokioChannelImpl,
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
    #[arg(long = "burst-id", required = false, default_value = "broadcast-bench")]
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

    let mut handles = vec![];
    let mut start_times = vec![];
    let mut end_times = vec![];
    let mut total_bytes = vec![];

    for (id, proxy) in proxies {
        let start_time = Arc::new(Mutex::new(Instant::now()));
        let end_time = Arc::new(Mutex::new(Instant::now()));
        let bytes = Arc::new(Mutex::new(0));

        start_times.push(start_time.clone());
        end_times.push(end_time.clone());
        total_bytes.push(bytes.clone());

        handles.push(thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let r = rt.block_on(async {
                let r = worker(proxy, args.payload_size, start_time, end_time, bytes).await;
                info!("runtime end: id={}", id);
                r
            });
            info!("thread end: id={}", id);
            r
        }));
    }

    for handle in handles {
        if let Err(e) = handle.join().unwrap() {
            error!("{:?}", e);
        }
        info!("join end");
    }

    info!("start_times: {:?}", start_times);
    info!("end_times: {:?}", end_times);

    let start = start_times
        .iter()
        .map(|x| *x.lock().unwrap())
        .min()
        .unwrap();
    let end = end_times.iter().map(|x| *x.lock().unwrap()).max().unwrap();
    let elapsed_time = end.duration_since(start).as_secs_f64();

    let total_bytes: usize = total_bytes.iter().map(|x| *x.lock().unwrap()).sum();

    let bandwidth = total_bytes as f64 / elapsed_time;

    let mbytesps = bandwidth / 1024.0 / 1024.0;

    info!("Total bytes: {}", total_bytes);
    info!("Duration: {:.3} s", elapsed_time);
    info!("Bandwidth: {:.3} MB/s", mbytesps);
}

async fn worker(
    burst_middleware: BurstMiddleware,
    payload: usize,
    start_time: Arc<Mutex<Instant>>,
    end_time: Arc<Mutex<Instant>>,
    total_bytes: Arc<Mutex<usize>>,
) -> Result<()> {
    let id = burst_middleware.info().worker_id;
    info!("worker start: id={}", id);

    let data = Bytes::from(vec![b'x'; payload]);

    //let mut elapsed_time = Duration::new(0, 0);
    let start = Instant::now();
    let mut start_time = start_time.lock().unwrap();
    *start_time = start.clone();
    drop(start_time); // Release the lock early

    let mddwr = burst_middleware.clone();

    // If id 0, receiver
    if id == 0 {
        let receive = tokio::spawn(async move {
            info!("Thread {} started receiving", id);
            let mut received_bytes = 0;

            while let Ok(Some(msgs)) = mddwr.gather(data.clone()).await {
                info!("Received {} messages: {:?}", msgs.len(), msgs);

                // check if ordered
                msgs.iter().enumerate().for_each(|(i, msg)| {
                    if msg.sender_id != i as u32 {
                        error!("Received data is not in order: {} != {}", msg.sender_id, i);
                    }
                });

                // Check if all empty except mine
                if msgs.iter().filter(|x| x.data.is_empty()).count()
                    == (burst_middleware.info().burst_size - 1) as usize
                {
                    info!(
                        "Received {} empty messages",
                        burst_middleware.info().burst_size - 1
                    );
                    break;
                }

                received_bytes += msgs.iter().map(|x| x.data.len()).sum::<usize>();
            }

            let mut total_bytes = total_bytes.lock().unwrap();
            *total_bytes = received_bytes;

            let mut end_time = end_time.lock().unwrap();
            *end_time = Instant::now();

            info!("Thread {} finished receiving", id);
        });
        receive.await?;
    // If id != 0, sender
    } else {
        let send = tokio::spawn(async move {
            info!("Thread {} started sending", id);
            // while elapsed_time < Duration::from_secs(duration) {
            if let Err(e) = burst_middleware.gather(data.clone()).await {
                error!("Error: {}", e);
            }
            // elapsed_time = start.elapsed();
            // info!("elapsed_time: {:?}", elapsed_time)
            // }

            // Signal the end of data transfer
            if let Err(e) = burst_middleware.gather(Bytes::new()).await {
                error!("Error: {}", e);
            }

            info!("Thread {} finished sending", id);
        });
        send.await?;
    }

    info!("worker end: id={}", id);

    Ok(())
}
