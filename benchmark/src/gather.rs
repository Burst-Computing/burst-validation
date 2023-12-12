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

    // If id 0, receiver
    if id == 0 {
        let mut received_bytes = 0;
        let mut received_messages = 0;
        let t0: Instant = Instant::now();

        info!("Worker {} - started receiving", id);
        for _ in 0..repeat {
            let msgs = burst_middleware
                .gather(data.clone())
                .await
                .unwrap()
                .unwrap();
            // skip the first message (self message)
            received_messages += msgs.len() - 1;
            received_bytes += msgs
                .into_iter()
                .skip(1)
                .fold(0, |acc, msg| acc + msg.data.len());
        }

        let t = t0.elapsed();
        let size_mb = received_bytes as f64 / 1024.0 / 1024.0;
        latency = t.as_millis() as f64 / repeat as f64 / 1000.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        info!(
            "Worker {} - received {} MB ({} messages) in {} s (latency: {} s, throughput {} MB/s)",
            id,
            size_mb,
            received_messages,
            t.as_millis() as f64 / 1000.0,
            latency,
            throughput
        );
    // If id != 0, sender
    } else {
        let t0: Instant = Instant::now();

        info!("Thread {} started sending", id);
        for _ in 0..repeat {
            burst_middleware.gather(data.clone()).await.unwrap();
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
