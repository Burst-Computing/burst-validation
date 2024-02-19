use burst_communication_middleware::MiddlewareActorHandle;
use bytes::Bytes;
use log::info;

use crate::{get_timestamp, Out};

pub fn worker(burst_middleware: MiddlewareActorHandle, payload: usize) -> Out {
    let id = burst_middleware.info.worker_id;
    info!("worker start: id={}", id);

    let data = Bytes::from(vec![b'x'; payload]);
    let data_all = (0..burst_middleware.info.burst_size - 1)
        .map(|_| data.clone())
        .collect::<Vec<Bytes>>();

    info!("Worker {} - started sending & receiving", id);

    let start = get_timestamp();
    let msgs = burst_middleware.all_to_all(data_all).unwrap();
    let end = get_timestamp();

    let received_messages = msgs.len();
    info!("Worker {} - received {} messages", id, received_messages);
    let total_size = msgs.into_iter().fold(0, |acc, msg| acc + msg.data.len());

    info!("worker {} end", id);

    let elapsed = end - start;
    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let throughput = size_mb as f64 / elapsed as f64;

    info!(
        "Worker {} - received {} MB in {} s (throughput {} MB/s)",
        id, size_mb, elapsed, throughput
    );

    Out {
        throughput,
        start,
        end,
    }
}
