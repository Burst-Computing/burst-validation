use core::panic;

use burst_communication_middleware::BurstMiddleware;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use sprs::{vec, CsMat, CsVec};

const ERR: f64 = 0.00001;
const DAMPING: f64 = 0.85;
const INIT_PAGE_RANK: f64 = 0.25;

const MASTER: i32 = 0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    bucket: String,
    key: String,
    nodes: u32,
    s3_config: S3Config,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3Config {
    region: String,
    endpoint: String,
    aws_access_key_id: String,
    aws_secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
}

// Example graph
// 0 -> 1, 3
// 1 -> 2, 4
// 2 -> 3
// 3 -> 4
// 4 -> 0, 1, 2

fn get_adjacency_matrix(worker_id: u32, init_value: f64) -> (Vec<i32>, Vec<i32>, Vec<f64>) {
    match worker_id {
        0 => (vec![0, 0, 1, 1], vec![1, 3, 2, 4], vec![init_value; 4]),
        1 => (vec![2, 3], vec![3, 4], vec![init_value; 2]),
        2 => (vec![4, 4, 4], vec![0, 1, 2], vec![init_value; 3]),
        _ => panic!("cowaboomer"),
    }
}

fn pagerank(params: Input, burst_middleware: &BurstMiddleware) -> Output {
    let rank = burst_middleware.info().worker_id;
    let num_pages = params.nodes;
    // Initialize vectors
    let page_ranks = vec![INIT_PAGE_RANK; params.nodes as usize];
    let damping = vec![DAMPING; params.nodes as usize];
    // page_values contains the non-zero values (page weight) of the page graph adjacency matrix assigned to this worker
    // row_indexes and col_indexes are the row and column indexes of the non-zero values
    let (row_indexes, col_indexes, page_values) = get_adjacency_matrix(rank, 1.0);
    let mut norm = 0.0;
    let mut sum = vec![0.0; params.nodes as usize];

    while norm < ERR {
        norm = 0.0;
        for i in 0..sum.len() {
            sum[i] = 0.0;
        }

        for page in 0..num_pages {
            for col in 0..2 {}
        }
    }

    unimplemented!();
}

// ow_main would be the entry point of an actual open whisk burst worker
pub fn main(args: Value, burst_middleware: BurstMiddleware) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    println!(
        "[Worker {}] Starting pagerank: {:?}",
        burst_middleware.info().worker_id,
        input
    );

    let result = pagerank(input, &burst_middleware);

    println!(
        "[Worker {}] Done: {:?}",
        burst_middleware.info().worker_id,
        result
    );
    serde_json::to_value(result)
}

// main function acts as a wrapper of what the OW runtime would do, used for debugging
// #[tokio::main]
// async fn main() {
//     let file = File::open("sort_payload.json").unwrap();
//     let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();

//     let burst_size = 4;
//     let mut group_ranges: HashMap<String, HashSet<u32>> = HashMap::new();
//     let range = (0..burst_size).collect::<HashSet<u32>>();
//     group_ranges.insert("0".to_string(), range);

//     let proxies = match BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
//         BurstOptions::new(
//             "terasort".to_string(),
//             burst_size,
//             group_ranges,
//             0.to_string(),
//         ),
//         TokioChannelOptions::new()
//             .broadcast_channel_size(256)
//             .build(),
//         RabbitMQOptions::new(inputs[0].rabbitmq_config.uri.clone())
//             .durable_queues(true)
//             .ack(true)
//             .build(),
//     )
//     .await
//     {
//         Ok(p) => p,
//         Err(e) => {
//             // error!("{:?}", e);
//             println!("{:?}", e);
//             panic!();
//         }
//     };

//     let mut threads = Vec::with_capacity(inputs.len());
//     for (proxy, input) in zip(proxies, inputs) {
//         let (idx, middleware) = proxy;
//         let thread = thread::spawn(move || {
//             println!("thread start: id={}", idx);
//             let result = ow_main(serde_json::to_value(input).unwrap(), middleware).unwrap();
//             println!("thread end: id={}", idx);
//             result
//         });
//         threads.push(thread);
//     }

//     for (i, t) in threads.into_iter().enumerate() {
//         let result = t.join().unwrap();
//         // write output to file, this would be the response of OW invokation
//         let file = File::create(format!("output_{}.json", i)).unwrap();
//         serde_json::to_writer(file, &result).unwrap();
//     }
// }
