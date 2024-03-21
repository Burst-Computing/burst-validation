use std::{collections::HashMap, fs, path::Path, thread, time::SystemTime};

use benchmark::{
    all_to_all, broadcast, gather, pair, scatter, setup_logging, Arguments, Benchmark, Out,
};
use burst_communication_middleware::create_actors;
use clap::Parser;
use csv::Writer;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    benchmark: String,
    backend: String,
    burst_id: String,
    burst_size: u32,
    groups: u32,
    granularity: u32,
    chunking: bool,
    chunk_size: usize,
    payload_size: usize,
    group_id: String,
    worker_id: u32,
    throughput: f64,
    start: f64,
    end: f64,
}

fn main() {
    let args = Arguments::parse();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let datetime = format!("{}", chrono::Local::now().format("%Y-%m-%d-%H-%M-%S"));
    let log = Path::new("results")
        .join(&datetime)
        .join(format!("group_{}.log", args.group_id));
    fs::create_dir_all(log.parent().unwrap()).unwrap();

    setup_logging(log);
    info!("{:?}", args);

    if (args.burst_size % 2) != 0 {
        panic!("Burst size must be even number");
    }

    info!("Running {:?} benchmark", args.benchmark);

    if args.benchmark == Benchmark::Pair {
        let data_per_worker = args.payload_size as usize;
        let total_data = data_per_worker * (args.burst_size / 2) as usize;
        info!(
            "Total data to transmit: {} MB ({} MB per worker)",
            total_data / 1024 / 1024,
            data_per_worker / 1024 / 1024
        );
    }

    let actors = match create_actors(args.clone().into(), &tokio_runtime) {
        Ok(proxies) => proxies,
        Err(e) => {
            panic!("Failed to create actors: {}", e);
        }
    };

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("start: {}", t.as_millis() as f64 / 1000.0);

    let mut threads = HashMap::with_capacity(actors.len());
    for (worker_id, actor) in actors {
        let payload_size = args.payload_size;
        let benchmark = args.benchmark.clone();
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            let result = match benchmark {
                Benchmark::Pair => pair::worker(actor, payload_size),
                Benchmark::Broadcast => broadcast::worker(actor, payload_size),
                Benchmark::AllToAll => all_to_all::worker(actor, payload_size),
                Benchmark::Gather => gather::worker(actor, payload_size),
                Benchmark::Scatter => scatter::worker(actor, payload_size),
            };
            info!("thread end: id={}", worker_id);
            return result;
        });
        threads.insert(worker_id, thread);
    }

    // append to csv in results directory (create if doesn't exist)
    let csv = Path::new("results")
        .join(&datetime)
        .join(format!("group_{}.csv", args.group_id));
    let mut writer = Writer::from_writer(
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(csv)
            .unwrap(),
    );

    let mut agg_throughput: f64 = 0.0;
    for (worker_id, thread) in threads {
        let Out {
            throughput,
            start,
            end,
        } = thread.join().unwrap();

        agg_throughput += throughput;

        let record = Record {
            benchmark: format!("{}", args.benchmark),
            backend: format!("{}", args.backend),
            burst_id: args.burst_id.clone(),
            burst_size: args.burst_size,
            groups: args.groups,
            granularity: args.burst_size / args.groups,
            chunking: args.chunking,
            chunk_size: args.chunk_size,
            payload_size: args.payload_size,
            group_id: args.group_id.clone(),
            worker_id,
            throughput,
            start,
            end,
        };

        writer.serialize(record).unwrap();
    }

    info!("Aggregated throughput: {} MB/s", agg_throughput);

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("end: {}", t.as_millis() as f64 / 1000.0);
}
