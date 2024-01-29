use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    fs::OpenOptions,
    path::Path,
    time::SystemTime,
};

use burst_communication_middleware::{
    BurstMessageRelayImpl, BurstMessageRelayOptions, BurstMiddleware, BurstOptions,
    MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions, RedisListImpl, RedisListOptions,
    RedisStreamImpl, RedisStreamOptions, TokioChannelImpl, TokioChannelOptions,
};
use clap::{Parser, Subcommand, ValueEnum};
use log::error;
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

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

    /// Enable chunking
    #[arg(long = "chunking", required = false, default_value = "false")]
    pub chunking: bool,

    /// Chunk size
    #[arg(long = "chunk-size", required = false, default_value = "1048576")] // 1MB
    pub chunk_size: usize,

    /// Tokio broadcast channel size
    #[arg(
        long = "tokio-broadcast-channel-size",
        required = false,
        default_value = "1048576"
    )]
    pub tokio_broadcast_channel_size: usize,
}

pub struct Out {
    pub throughput: f64,
    pub start: f64,
    pub end: f64,
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
    /// Use Redis Streams as backend
    RedisStream,
    /// Use Redis Lists as backend
    RedisList,
    /// Use RabbitMQ as backend
    Rabbitmq,
    /// Use burst message relay as backend
    MessageRelay,
}

impl Display for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::S3 {
                bucket: _,
                region: _,
                access_key_id: _,
                secret_access_key: _,
                session_token: _,
            } => {
                write!(f, "S3")?;
            }
            Backend::RedisStream => {
                write!(f, "RedisStream")?;
            }
            Backend::RedisList => {
                write!(f, "RedisList")?;
            }
            Backend::Rabbitmq => {
                write!(f, "RabbitMQ")?;
            }
            Backend::MessageRelay => {
                write!(f, "BurstMessageRelay")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, ValueEnum)]
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

impl Display for Benchmark {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Benchmark::Pair => {
                write!(f, "Pair")?;
            }
            Benchmark::Broadcast => {
                write!(f, "Broadcast")?;
            }
            Benchmark::Scatter => {
                write!(f, "Scatter")?;
            }
            Benchmark::Gather => {
                write!(f, "Gather")?;
            }
            Benchmark::AllToAll => {
                write!(f, "AllToAll")?;
            }
        }
        Ok(())
    }
}

pub fn create_proxies(
    args: &Arguments,
    tokio_runtime: &tokio::runtime::Runtime,
) -> HashMap<u32, MiddlewareActorHandle> {
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
        args.chunking,
        args.chunk_size,
    );

    let channel_options = TokioChannelOptions::new()
        .broadcast_channel_size(args.tokio_broadcast_channel_size)
        .build();

    let proxies = tokio_runtime.block_on(async move {
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
            Backend::RedisStream => {
                let mut options = RedisStreamOptions::default();
                if let Some(server) = &args.server {
                    options.redis_uri(server.to_string());
                }
                proxies =
                    BurstMiddleware::create_proxies::<TokioChannelImpl, RedisStreamImpl, _, _>(
                        burst_options,
                        channel_options,
                        options,
                    )
                    .await;
            }
            Backend::RedisList => {
                let mut options = RedisListOptions::default();
                if let Some(server) = &args.server {
                    options.redis_uri(server.to_string());
                }
                proxies = BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
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
        return proxies;
    });

    let actors = match proxies {
        Ok(p) => p
            .into_iter()
            .map(|(id, proxy)| {
                let actor = MiddlewareActorHandle::new(proxy, tokio_runtime);
                (id, actor)
            })
            .collect::<HashMap<u32, MiddlewareActorHandle>>(),
        Err(e) => {
            error!("{:?}", e);
            panic!("Failed to create proxies");
        }
    };

    actors
}

pub fn setup_logging(log: impl AsRef<Path>) {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().with_writer(file).with_ansi(false))
        .with(fmt::layer())
        .init();
}

pub fn get_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64
        / 1000.0
}
