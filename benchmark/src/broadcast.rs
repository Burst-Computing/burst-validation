use burst_communication_middleware::BurstMiddleware;
use bytes::Bytes;
use log::info;

use crate::{get_timestamp, Out};

pub async fn worker(burst_middleware: BurstMiddleware, payload: usize) -> Out {
    let id = burst_middleware.info().worker_id;
    info!("worker start: id={}", id);
    let start = get_timestamp();

    let total_size;

    // If id 0, sender
    if id == 0 {
        let data = Bytes::from(vec![b'x'; payload]);
        total_size = data.len();
        info!("Worker {} - started sending", id);
        burst_middleware.broadcast(Some(data)).await.unwrap();
    // If id != 0, receiver
    } else {
        info!("Worker {} - started receiving", id);
        let msg = burst_middleware.broadcast(None).await.unwrap();
        total_size = msg.data.len();
    }

    info!("worker {} end", id);
    let end = get_timestamp();

    let elapsed = end - start;
    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let throughput = size_mb as f64 / elapsed as f64;

    if id == 0 {
        info!(
            "Worker {} - sent {} MB in {} s (throughput {} MB/s)",
            id, size_mb, elapsed, throughput
        );
    } else {
        info!(
            "Worker {} - received {} MB in {} s (throughput {} MB/s)",
            id, size_mb, elapsed, throughput
        );
    }

    Out {
        throughput,
        start,
        end,
    }
}
