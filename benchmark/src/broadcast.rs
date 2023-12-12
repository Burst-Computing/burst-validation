use burst_communication_middleware::BurstMiddleware;
use bytes::Bytes;
use log::info;
use std::time::Instant;

use crate::{get_timestamp, Out};

pub async fn worker(burst_middleware: BurstMiddleware, payload: usize, repeat: u32) -> Out {
    let id = burst_middleware.info().worker_id;
    info!("worker start: id={}", id);

    let data = Bytes::from(vec![b'x'; payload]);

    let latency;
    let throughput;
    let start = get_timestamp();

    // If id 0, sender
    if id == 0 {
        let t0: Instant = Instant::now();

        info!("Worker {} - started sending", id);
        for _ in 0..repeat {
            burst_middleware
                .broadcast(Some(data.clone()))
                .await
                .unwrap();
        }

        let t = t0.elapsed();
        let total_size = data.len() * repeat as usize;
        let size_mb = total_size as f64 / 1024.0 / 1024.0;
        latency = t.as_millis() as f64 / repeat as f64 / 1000.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - sent {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            latency,
            throughput
        );
    // If id != 0, receiver
    } else {
        let mut received_bytes = 0;
        let t0: Instant = Instant::now();

        info!("Worker {} - started receiving", id);
        for _ in 0..repeat {
            let msg = burst_middleware.broadcast(None).await.unwrap();
            received_bytes += msg.data.len();
        }

        let t = t0.elapsed();
        let size_mb = received_bytes as f64 / 1024.0 / 1024.0;
        latency = t.as_millis() as f64 / repeat as f64 / 1000.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - received {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            latency,
            throughput
        );
    }

    info!("worker {} end", id);

    let end = get_timestamp();

    Out {
        latency,
        throughput,
        start,
        end,
    }
}
