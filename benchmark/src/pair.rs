use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use log::info;

use crate::{get_timestamp, Out};

pub fn worker(burst_middleware: MiddlewareActorHandle, payload: usize) -> Out {
    let id = burst_middleware.info.worker_id;
    let burst_size = burst_middleware.info.burst_size;
    info!("worker start: id={}", id);

    let total_size;
    let start;
    let end;

    // If id < burst_size / 2, receiver
    if id < (burst_size / 2) {
        let from = id + (burst_size / 2);

        info!("Worker {} - started receiving from {}", id, from);
        start = get_timestamp();
        let msg = burst_middleware.recv(from).unwrap();
        end = get_timestamp();
        total_size = msg.data.len();

    // If id >= burst_size / 2, sender
    } else {
        let data = Bytes::from(vec![b'x'; payload]);
        let target = id % (burst_size / 2);
        total_size = data.len();

        info!("Worker {} - started sending to {}", id, target);
        start = get_timestamp();
        burst_middleware.send(target, data).unwrap();
        end = get_timestamp();
    }

    info!("worker {} end", id);

    let elapsed = end - start;
    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let throughput = size_mb / elapsed;

    if id < (burst_size / 2) {
        info!(
            "Worker {} - received {} MB in {} s (throughput {} MB/s)",
            id, size_mb, elapsed, throughput
        );
    } else {
        info!(
            "Worker {} - sent {} MB in {} s (throughput {} MB/s)",
            id, size_mb, elapsed, throughput
        );
    }

    Out {
        throughput,
        start,
        end,
    }
}
