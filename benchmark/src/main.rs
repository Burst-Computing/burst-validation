use std::{fs, path::Path, time::SystemTime};

use benchmark::{
    all_to_all, broadcast, create_proxies, create_threads, gather, pair, scatter, setup_logging,
    Arguments, Benchmark,
};
use clap::Parser;
use csv::Writer;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    benchmark: String,
    backend: String,
    burst_size: u32,
    groups: u32,
    granularity: u32,
    group_id: String,
    worker_id: u32,
    latency: f64,
    throughput: f64,
}

#[tokio::main]
async fn main() {
    setup_logging();

    let args = Arguments::parse();
    info!("{:?}", args);

    if (args.burst_size % 2) != 0 {
        panic!("Burst size must be even number");
    }

    info!("Running {:?} benchmark", args.benchmark);

    if args.benchmark == Benchmark::Pair {
        let data_per_worker = args.payload_size * args.repeat as usize;
        let total_data = data_per_worker * (args.burst_size / 2) as usize;
        info!(
            "Total data to transmit: {} MB ({} MB per worker)",
            total_data / 1024 / 1024,
            data_per_worker / 1024 / 1024
        );
    }

    let proxies = create_proxies(&args).await;

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("start: {}", t.as_millis() as f64 / 1000.0);

    let threads = match &args.benchmark {
        Benchmark::Pair => create_threads(&args, proxies, pair::worker),
        Benchmark::Broadcast => create_threads(&args, proxies, broadcast::worker),
        Benchmark::AllToAll => create_threads(&args, proxies, all_to_all::worker),
        Benchmark::Gather => create_threads(&args, proxies, gather::worker),
        Benchmark::Scatter => create_threads(&args, proxies, scatter::worker),
    };

    // append to csv in results directory (create if doesn't exist)
    let datetime = format!("{}", chrono::Local::now().format("%d-%m-%Y_%H-%M-%S"));
    let path = Path::new("results")
        .join(datetime)
        .join(format!("group_{}.csv", args.group_id));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer = Writer::from_writer(
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path)
            .unwrap(),
    );

    let mut total_latency: f64 = 0.0;
    let mut agg_throughput: f64 = 0.0;
    for (worker_id, thread) in threads {
        let (latency, throughput) = thread.join().unwrap();
        total_latency += latency;
        agg_throughput += throughput;

        let record = Record {
            benchmark: format!("{}", args.benchmark),
            backend: format!("{}", args.backend),
            burst_size: args.burst_size,
            groups: args.groups,
            granularity: args.burst_size / args.groups,
            group_id: args.group_id.clone(),
            worker_id,
            latency,
            throughput,
        };

        writer.serialize(record).unwrap();
    }

    info!(
        "Average latency: {} s, aggregate throughput: {} MB/s",
        total_latency / args.burst_size as f64,
        agg_throughput
    );

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("end: {}", t.as_millis() as f64 / 1000.0);
}
