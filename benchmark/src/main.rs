use std::time::SystemTime;

use benchmark::{
    all_to_all, broadcast, create_proxies, create_threads, gather, pair, scatter, setup_logging,
    Arguments, Benchmark,
};
use clap::Parser;
use log::info;

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
        Benchmark::Pair => create_threads(args, proxies, pair::worker),
        Benchmark::Broadcast => create_threads(args, proxies, broadcast::worker),
        Benchmark::AllToAll => create_threads(args, proxies, all_to_all::worker),
        Benchmark::Gather => create_threads(args, proxies, gather::worker),
        Benchmark::Scatter => create_threads(args, proxies, scatter::worker),
    };

    let mut agg_throughput: f64 = 0.0;
    for thread in threads {
        let throughput = thread.join().unwrap();
        agg_throughput += throughput;
    }

    info!("Total throughput: {} MB/s", agg_throughput as f64);

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("end: {}", t.as_millis() as f64 / 1000.0);
}
