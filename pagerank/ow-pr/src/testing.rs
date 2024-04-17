use actions::{main as ow_main, PagerankMessage};
use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, RedisListImpl, RedisListOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use log::info;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    thread,
};

// Burst options
const GRANULARITY: u32 = 1;
const INPUT_JSON_PARAMS: &str = "pagerank_payload.json";

// Middleware options
const ENABLE_CHUNKING: bool = true;
const MESSAGE_CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1MB

fn main() {
    // env_logger::init();

    let input_json_file = File::open(INPUT_JSON_PARAMS).unwrap();
    let params: Vec<Value> = serde_json::from_reader(input_json_file).unwrap();

    println!("params: {:?}", params);

    let burst_size = params.len() as u32;
    println!("burst_size: {}", burst_size);

    if burst_size % GRANULARITY != 0 {
        panic!(
            "BURST_SIZE {} must be divisible by GRANULARITY {}",
            burst_size, GRANULARITY
        );
    }

    let num_groups = burst_size / GRANULARITY;
    println!("num_groups: {}", num_groups);
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

    let mut actors = (0..num_groups)
        .flat_map(|group_id| {
            let burst_options =
                BurstOptions::new(burst_size, group_ranges.clone(), group_id.to_string())
                    .burst_id("pagerank".to_string())
                    .enable_message_chunking(ENABLE_CHUNKING)
                    .message_chunk_size(MESSAGE_CHUNK_SIZE)
                    .build();
            let channel_options = TokioChannelOptions::new()
                .broadcast_channel_size(256)
                .build();
            let backend_options = RedisListOptions::new("redis://127.0.0.1".to_string()).build();

            let fut = BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
                burst_options,
                channel_options,
                backend_options,
            );
            let proxies = tokio_runtime.block_on(fut).unwrap();

            proxies
                .into_iter()
                .map(|(worker_id, middleware)| {
                    (
                        worker_id,
                        Middleware::new(middleware, tokio_runtime.handle().clone()),
                    )
                })
                .collect::<HashMap<u32, Middleware<PagerankMessage>>>()
        })
        .collect::<Vec<_>>();

    // sort actors by worker_id so that the order of the workers and the ordered params is correct
    actors.sort_by(|(a, _), (b, _)| a.cmp(b));

    let threads = actors
        .into_iter()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                info!("thread start: id={}", worker_id);
                let result = ow_main(param, proxy);
                info!("thread end: id={}", worker_id);
                result
            })
        })
        .collect::<Vec<_>>();

    let mut results = Vec::with_capacity(threads.len());
    for thread in threads {
        let worker_result = thread.join().unwrap().unwrap();
        results.push(worker_result);
    }

    let output_file = File::create("pagerank_output.json").unwrap();
    serde_json::to_writer(output_file, &results).unwrap();
}
