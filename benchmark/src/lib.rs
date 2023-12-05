use std::{
    collections::{HashMap, HashSet},
    future::Future,
    thread::{self, JoinHandle},
};

use burst_communication_middleware::{
    BurstMessageRelayImpl, BurstMessageRelayOptions, BurstMiddleware, BurstOptions, RabbitMQMImpl,
    RabbitMQOptions, RedisImpl, RedisOptions, TokioChannelImpl, TokioChannelOptions,
};
use clap::{Parser, Subcommand, ValueEnum};
use log::{error, info};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub mod all_to_all;
pub mod broadcast;
pub mod gather;
pub mod pair;
pub mod scatter;

#[derive(Parser, Debug)]
pub struct Arguments {
    /// Benchmark to run
    #[arg(value_enum, long = "benchmark", required = true)]
    pub benchmark: Benchmark,

    /// Backend
    #[command(subcommand)]
    pub backend: Backend,

    /// Server address, URI or endpoint
    #[arg(long = "server", required = false)]
    pub server: Option<String>,

    /// Burst ID
    #[arg(long = "burst-id", required = false, default_value = "burst")]
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

    /// Tokio broadcast channel size
    #[arg(
        long = "tokio-broadcast-channel-size",
        required = false,
        default_value = "1048576"
    )]
    pub tokio_broadcast_channel_size: usize,
}

#[derive(Debug, Subcommand)]
pub enum Backend {
    /// Use S3 as backend
    S3 {
        /// S3 bucket name
        #[arg(long = "bucket", required = false)]
        bucket: Option<String>,
        /// S3 region
        #[arg(long = "region", required = false)]
        region: Option<String>,
        /// S3 access key id
        #[arg(long = "access-key-id", required = false)]
        access_key_id: Option<String>,
        /// S3 secret access key
        #[arg(long = "secret-access-key", required = false)]
        secret_access_key: Option<String>,
        /// S3 session token
        #[arg(long = "session-token", required = false)]
        session_token: Option<String>,
    },
    /// Use Redis as backend
    Redis,
    /// Use RabbitMQ as backend
    Rabbitmq,
    /// Use burst message relay as backend
    MessageRelay,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum Benchmark {
    /// Run pair benchmark
    Pair,
    /// Run broadcast benchmark
    Broadcast,
    /// Run scatter benchmark
    Scatter,
    /// Run gather benchmark
    Gather,
    /// Run all-to-all benchmark
    AllToAll,
}

pub fn create_threads<F, Fut>(
    args: Arguments,
    proxies: HashMap<u32, BurstMiddleware>,
    f: F,
) -> Vec<JoinHandle<f64>>
where
    F: FnOnce(BurstMiddleware, usize, u32) -> Fut + Copy + Send + 'static,
    Fut: Future<Output = f64> + Send + 'static,
{
    let mut threads = Vec::with_capacity(proxies.len());
    for (worker_id, proxy) in proxies {
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = tokio_runtime.block_on(f(proxy, args.payload_size, args.repeat));
            info!("thread end: id={}", worker_id);
            result
        });
        threads.push(thread);
    }
    threads
}

pub async fn create_proxies(args: &Arguments) -> HashMap<u32, BurstMiddleware> {
    let group_size = args.burst_size / args.groups;

    let group_ranges = (0..args.groups)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((group_size * group_id)..((group_size * group_id) + group_size)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let burst_options = BurstOptions::new(
        args.burst_id.to_string(),
        args.burst_size,
        group_ranges,
        args.group_id.to_string(),
    );

    let channel_options = TokioChannelOptions::new()
        .broadcast_channel_size(args.tokio_broadcast_channel_size)
        .build();

    let proxies;
    match &args.backend {
        Backend::S3 {
            bucket,
            region,
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            let mut options = burst_communication_middleware::S3Options::default();
            if let Some(bucket) = bucket {
                options.bucket(bucket.to_string());
            }
            if let Some(region) = region {
                options.region(region.to_string());
            }
            if let Some(access_key_id) = access_key_id {
                options.access_key_id(access_key_id.to_string());
            }
            if let Some(secret_access_key) = secret_access_key {
                options.secret_access_key(secret_access_key.to_string());
            }
            options.session_token(session_token.clone());
            options.endpoint(args.server.clone());

            proxies = BurstMiddleware::create_proxies::<
                TokioChannelImpl,
                burst_communication_middleware::S3Impl,
                _,
                _,
            >(burst_options, channel_options, options)
            .await;
        }
        Backend::Redis => {
            let mut options = RedisOptions::default();
            if let Some(server) = &args.server {
                options.redis_uri(server.to_string());
            }
            proxies = BurstMiddleware::create_proxies::<TokioChannelImpl, RedisImpl, _, _>(
                burst_options,
                channel_options,
                options,
            )
            .await;
        }
        Backend::Rabbitmq => {
            let mut options = RabbitMQOptions::default()
                .durable_queues(true)
                .ack(true)
                .build();
            if let Some(server) = &args.server {
                options.rabbitmq_uri(server.to_string());
            }
            proxies = BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
                burst_options,
                channel_options,
                options,
            )
            .await;
        }
        Backend::MessageRelay => {
            let mut options = BurstMessageRelayOptions::default();
            if let Some(server) = &args.server {
                options.server_uri(server.to_string());
            }
            proxies = BurstMiddleware::create_proxies::<
                TokioChannelImpl,
                BurstMessageRelayImpl,
                _,
                _,
            >(burst_options, channel_options, options)
            .await;
        }
    }

    let proxies = match proxies {
        Ok(p) => p,
        Err(e) => {
            error!("{:?}", e);
            panic!("Failed to create proxies");
        }
    };

    proxies
}

pub fn setup_logging() {
    if let Err(err) = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
    {
        eprintln!("Failed to initialize logging: {}", err);
    }
}
