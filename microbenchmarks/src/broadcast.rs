use burst_communication_middleware::Middleware;
use bytes::Bytes;
use log::info;

use crate::{get_timestamp, Out};

const ROOT: u32 = 0;

pub fn worker(burst_middleware: Middleware<Bytes>, payload: usize) -> Out {
    let burst_middleware = burst_middleware.get_actor_handle();
    let id = burst_middleware.info.worker_id;
    info!("worker start: id={}", id);

    let total_size;
    let start;
    let end;

    // If id 0, sender
    if id == 0 {
        let data = Bytes::from(vec![b'x'; payload]);
        total_size = data.len();
        info!("Worker {} - started sending", id);
        start = get_timestamp();
        burst_middleware.broadcast(Some(data), ROOT).unwrap();
        end = get_timestamp();
    // If id != 0, receiver
    } else {
        info!("Worker {} - started receiving", id);
        start = get_timestamp();
        let msg = burst_middleware.broadcast(None, ROOT).unwrap();
        end = get_timestamp();
        total_size = msg.len();
    }

    info!("worker {} end", id);

    let elapsed = end - start;
    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let throughput = size_mb / elapsed;

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
