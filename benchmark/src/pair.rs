use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use log::{debug, info};
use std::time::Instant;

use crate::{get_timestamp, Out};

pub fn worker(burst_middleware: MiddlewareActorHandle, payload: usize, repeat: u32) -> Out {
    info!("worker {} start", burst_middleware.info.worker_id);
    let latency;
    let throughput;
    let start = get_timestamp();

    // If id < burst_size / 2, receiver
    if burst_middleware.info.worker_id < (burst_middleware.info.burst_size / 2) {
        let mut total_size = 0;
        let from = burst_middleware.info.worker_id + (burst_middleware.info.burst_size / 2);

        let msg = burst_middleware.recv(from).unwrap();
        total_size += msg.data.len();

        let t0: Instant = Instant::now();

        log::info!(
            "Worker {} - started receiving",
            burst_middleware.info.worker_id
        );
        for _ in 0..repeat - 1 {
            let msg = burst_middleware.recv(from).unwrap();
            total_size += msg.data.len();
        }
        let t = t0.elapsed();
        let size_mb = total_size as f64 / 1024.0 / 1024.0;
        latency = t.as_millis() as f64 / repeat as f64 / 1000.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - received {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            burst_middleware.info.worker_id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            latency,
            throughput
        );
    // If id >= burst_size / 2, sender
    } else {
        let data = Bytes::from(vec![b'x'; payload]);
        let target = burst_middleware.info.worker_id % (burst_middleware.info.burst_size / 2);
        debug!(
            "Worker {} Sending to {}",
            burst_middleware.info.worker_id, target
        );
        let t0 = Instant::now();
        for _ in 0..repeat {
            burst_middleware.send(target, data.clone()).unwrap();
        }
        let t = t0.elapsed();
        let total_size = data.len() * repeat as usize;
        let size_mb = total_size as f64 / 1024.0 / 1024.0;
        latency = t.as_millis() as f64 / repeat as f64 / 1000.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - sent {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            burst_middleware.info.worker_id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            latency,
            throughput
        );
    }

    info!("worker {} end", burst_middleware.info.worker_id);

    let end = get_timestamp();

    Out {
        latency,
        throughput,
        start,
        end,
    }
}
