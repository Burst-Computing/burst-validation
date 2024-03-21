use actions::main as ow_main;
use burst_communication_middleware::{create_actors, Backend, Config};
use log::info;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
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
            create_actors(
                Config {
                    backend: Backend::RedisList,
                    server: Some("redis://localhost:6379".to_string()),
                    burst_id: "terasort".to_string(),
                    burst_size,
                    group_ranges: group_ranges.clone(),
                    group_id: group_id.to_string(),
                    chunking: ENABLE_CHUNKING,
                    chunk_size: CHUNK_SIZE,
                    tokio_broadcast_channel_size: Some(256),
                },
                &tokio_runtime,
            )
        })
        .collect::<Vec<_>>();

    let threads = proxies
        .into_iter()
        .flatten()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                info!("thread start: id={}", worker_id);
                let r = ow_main(param, proxy);
                info!("thread end: id={}", worker_id);
                r
            })
        })
        .collect::<Vec<_>>();
    for t in threads {
        t.join().unwrap().unwrap();
    }
}
