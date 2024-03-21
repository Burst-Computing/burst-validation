use actions::main as ow_main;
use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions,
    RedisListImpl, RedisListOptions, RedisStreamImpl, RedisStreamOptions, S3Impl, S3Options,
    TokioChannelImpl, TokioChannelOptions,
};
use log::info;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    ops::Add,
    thread,
};

const GRANULARITY: u32 = 4;
const INPUT_JSON_PARAMS: &str = "sort_payload.json";
const ENABLE_CHUNKING: bool = false;
const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1MB

fn main() {
    // env_logger::init();

    let input_json_file = File::open(INPUT_JSON_PARAMS).unwrap();
    let params: Vec<Value> = serde_json::from_reader(input_json_file).unwrap();

    let burst_size = params.len() as u32;

    if burst_size % GRANULARITY != 0 {
        panic!("BURST_SIZE must be divisible by GRANULARITY");
    }

    let num_groups = burst_size / GRANULARITY;
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let group_ranges = (0..num_groups)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((GRANULARITY * group_id)..((GRANULARITY * group_id) + GRANULARITY)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let proxies = (0..num_groups)
        .flat_map(|group_id| {
            let burst_options =
                BurstOptions::new(burst_size, group_ranges.clone(), group_id.to_string())
                    .burst_id("pagerank".to_string())
                    .enable_message_chunking(ENABLE_CHUNKING)
                    .message_chunk_size(CHUNK_SIZE)
                    .build();
            let channel_options = TokioChannelOptions::new()
                .broadcast_channel_size(256)
                .build();
            let backend_options =
                RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
                    .durable_queues(true)
                    .ack(true)
                    .build();

            tokio_runtime
                .block_on(BurstMiddleware::create_proxies::<
                    TokioChannelImpl,
                    RabbitMQMImpl,
                    _,
                    _,
                >(
                    burst_options, channel_options, backend_options
                ))
                .unwrap()
        })
        .collect::<Vec<_>>();

    let threads = proxies
        .into_iter()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                info!("thread start: id={}", worker_id);
                ow_main(param, proxy);
                info!("thread end: id={}", param.id);
            })
        })
        .collect::<Vec<_>>();
}
