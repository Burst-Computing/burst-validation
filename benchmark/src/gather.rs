use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use log::info;

use crate::{get_timestamp, Out};

pub fn worker(burst_middleware: MiddlewareActorHandle, payload: usize) -> Out {
    let id = burst_middleware.info.worker_id;
    info!("worker start: id={}", id);

    let total_size;
    let data = Bytes::from(vec![b'x'; payload]);
    let start;
    let end;

    // If id 0, receiver
    if id == 0 {
        info!("Worker {} - started receiving", id);
        start = get_timestamp();
        let msgs = burst_middleware.gather(data).unwrap().unwrap();
        end = get_timestamp();

        let received_messages = msgs.len();
        info!("Worker {} - received {} messages", id, received_messages);
        total_size = msgs.into_iter().fold(0, |acc, msg| acc + msg.data.len());
    // If id != 0, sender
    } else {
        info!("Worker {} - started sending", id);
        start = get_timestamp();
        burst_middleware.gather(data.clone()).unwrap();
        end = get_timestamp();
        total_size = data.len();
    }

    info!("worker {} end", id);

    let elapsed = end - start;
    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let throughput = size_mb / elapsed;

    if id == 0 {
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
