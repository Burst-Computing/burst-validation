use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::io::AsyncBufReadExt;

const ROOT_WORKER: u32 = 0;
const MAX_ITER: u32 = 100;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    input_data: S3InputParams,
    error: f64,
    num_nodes: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3InputParams {
    bucket: String,
    key: String,
    region: String,
    endpoint: Option<String>,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    timestamps: Vec<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Timestamp {
    key: String,
    value: u128,
}

fn timestamp(key: String) -> Timestamp {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH).unwrap();
    let milliseconds_timestamp = duration_since_epoch.as_millis();
    Timestamp {
        key,
        value: milliseconds_timestamp,
    }
}

#[derive(Debug, Clone)]
pub struct PagerankMessage(Vec<f64>);

impl From<Bytes> for PagerankMessage {
    fn from(bytes: Bytes) -> Self {
        // println!("Cast from bytes, size is {:?}", bytes.len());

        let mut vecu8 = bytes.to_vec();
        let vecf64 = unsafe {
            let ratio = std::mem::size_of::<f64>() / std::mem::size_of::<u8>();
            let length = vecu8.len() / ratio;
            let capacity = vecu8.capacity() / ratio;
            let ptr = vecu8.as_mut_ptr() as *mut f64;

            // Don't run the destructor for vec32
            std::mem::forget(vecu8);

            // Construct new Vec
            Vec::from_raw_parts(ptr, length, capacity)
        };
        PagerankMessage(vecf64)
    }
}

impl From<PagerankMessage> for Bytes {
    fn from(mut val: PagerankMessage) -> Self {
        let vec8 = unsafe {
            let ratio = std::mem::size_of::<f64>() / std::mem::size_of::<u8>();
            let length = val.0.len() * ratio;
            let capacity = val.0.capacity() * ratio;
            let ptr = val.0.as_mut_ptr() as *mut u8;

            // Don't run the destructor for vec32
            std::mem::forget(val.0);

            // Construct new Vec
            Vec::from_raw_parts(ptr, length, capacity)
        };
        let b = Bytes::from(vec8);
        // println!("Cast to bytes, size is {:?}", b.len());
        b
    }
}

async fn get_adjacency_matrix(
    params: &Input,
    s3_client: &S3Client,
) -> (HashMap<u32, HashSet<u32>>, (u32, u32)) {
    let buf_read = s3_client
        .get_object()
        .bucket(params.input_data.bucket.clone())
        .key(params.input_data.key.clone())
        .send()
        .await
        .unwrap()
        .body
        .into_async_read();

    let mut lines = buf_read.lines();

    let mut edges: HashMap<u32, HashSet<u32>> = HashMap::new();
    let mut min_node: u32 = u32::MAX;
    let mut max_node: u32 = 0;

    while let Some(line) = lines.next_line().await.unwrap() {
        let parts: Vec<&str> = line.split('\t').collect();
        let node = parts[0].parse::<u32>().unwrap();

        let edge = parts[1].parse::<u32>().unwrap();

        if let Some(edges_set) = edges.get_mut(&node) {
            edges_set.insert(edge);
        } else {
            if node < min_node {
                min_node = node;
            }
            if node > max_node {
                max_node = node;
            }
            let mut edges_set = HashSet::new();
            edges_set.insert(edge);
            edges.insert(node, edges_set);
        }
    }

    // println!("{:?}", edges);

    (edges, (min_node, max_node + 1))
}

fn pagerank(params: Input, burst_middleware: &MiddlewareActorHandle<PagerankMessage>) -> Output {
    let mut timestamps = Vec::new();
    timestamps.push(timestamp("worker_start".to_string()));

    let worker = burst_middleware.info.worker_id;

    // create s3 client
    let credentials_provider = Credentials::from_keys(
        params.input_data.aws_access_key_id.clone(),
        params.input_data.aws_secret_access_key.clone(),
        params.input_data.aws_session_token.clone(),
    );

    let config = match params.input_data.endpoint.clone() {
        Some(endpoint) => {
            aws_sdk_s3::config::Builder::new()
                .endpoint_url(endpoint)
                .credentials_provider(credentials_provider)
                .region(Region::new(params.input_data.region.clone()))
                .force_path_style(true) // apply bucketname as path param instead of pre-domain
                .build()
        }
        None => aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials_provider)
            .region(Region::new(params.input_data.region.clone()))
            .build(),
    };
    let s3_client = S3Client::from_conf(config);

    // create tokio runtime for async s3 requests
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // page_ranks contains the page rank of all nodes, where each index corresponds to the node id
    let init_pagerank = 1.0 / params.num_nodes as f64;
    let mut page_ranks = vec![init_pagerank; params.num_nodes as usize];
    let mut norm = f64::MAX;
    let mut iter = 0;
    let mut sum = vec![0.0; params.num_nodes as usize];
    // graph contains the adjacency matrix of the nodes assigned to this worker
    let (graph, nodes_range) = tokio_runtime.block_on(get_adjacency_matrix(&params, &s3_client));

    timestamps.push(timestamp("get_input".to_string()));
    println!(
        "[Worker {}] Graph size: {:?} (range: {:?})",
        worker,
        graph.len(),
        nodes_range
    );

    // calculate the number of outgoing links for each page of this worker
    println!("[Worker {}] Calculating outgoing links", worker);
    let mut out_links = HashMap::new();
    for i in nodes_range.0..nodes_range.1 {
        if let Some(edges) = graph.get(&i) {
            out_links.insert(i, edges.len());
        }
    }
    // println!("[Worker {}] Outgoing links: {:?}", worker, out_links);
    timestamps.push(timestamp("calc_outlinks".to_string()));

    while (norm >= params.error) && (iter < MAX_ITER) {
        let iter_t0 = SystemTime::now();
        println!("[Worker {}] *** Starting iteration {} ***", worker, iter);
        timestamps.push(timestamp(format!("iter_{}_start", iter)));

        // while iter < MAX_ITER {
        let pr_msg = if burst_middleware.info.worker_id == ROOT_WORKER {
            println!("[Worker {}] Broadcast page rank weights", worker);
            burst_middleware
                .broadcast(Some(PagerankMessage(page_ranks.clone())), ROOT_WORKER)
                .unwrap()
        } else {
            println!("[Worker {}] Waiting for updated page rank weights", worker);
            burst_middleware.broadcast(None, ROOT_WORKER).unwrap()
        };
        page_ranks = pr_msg.0;
        println!("[Worker {}] Received updated page ranks", worker);

        // println!("[Worker {}] Page ranks: {:?}", worker, page_ranks);
        timestamps.push(timestamp(format!("iter_{}_broadcast_weights", iter)));

        // for i in nodes_range.0..nodes_range.1 {
        //     println!("[Worker {}] Calculating sum for node {}", worker, i);
        //     for j in 0..params.num_nodes {
        //         if let Some(edges) = graph.get(&i) {
        //             if edges.contains(&j) {
        //                 let n_out_links = *out_links.get(&i).unwrap_or(&1) as f64;
        //                 sum[j as usize] += page_ranks[i as usize] / n_out_links;
        //             }
        //         }
        //     }
        // }

        // println!("{:?} {:?}", sum.len(), page_ranks.len());

        for (node, links) in graph.iter() {
            // println!("[Worker {}] Calculating sum for node {}", worker, node);
            for link in links.iter() {
                // println!("{:?}", link);
                let n_out_links = *out_links.get(node).unwrap_or(&1) as f64;
                sum[*link as usize] += page_ranks[*node as usize] / n_out_links;
            }
        }

        // println!("[Worker {}] Sums: {:?}", worker, sum);
        timestamps.push(timestamp(format!("iter_{}_calc_sums", iter)));

        let sum_msg = burst_middleware
            .reduce(PagerankMessage(sum.clone()), |vec1, vec2| {
                let reduced_sum = vec1
                    .0
                    .iter()
                    .zip(vec2.0.iter())
                    .map(|(a, b)| a + b)
                    .collect();
                PagerankMessage(reduced_sum)
            })
            .unwrap();
        timestamps.push(timestamp(format!("iter_{}_reduce", iter)));

        let new_norm = if let Some(x) = sum_msg {
            println!("[Worker {}] ROOT --> Received reduced sum", worker);
            // println!("Reduced sum: {:?}", x.0);

            // Root worker has the reduced sum of all ranks
            let new_page_ranks = x.0;
            // Calculate the difference between the new and old page ranks, sum the squares and take the square root
            norm = new_page_ranks
                .iter()
                .zip(page_ranks.iter())
                .map(|(a, b)| a - b)
                .map(|x| x * x)
                .sum::<f64>()
                .sqrt();

            page_ranks = new_page_ranks;
            println!("[Worker {}] ROOT --> New error is {}", worker, norm);
            // println!("Page Ranks: {:?}", page_ranks);
            Some(PagerankMessage(vec![norm]))
        } else {
            None
        };

        println!("[Worker {}] Broadcast new error to all workers", worker);
        norm = burst_middleware
            .broadcast(new_norm, ROOT_WORKER)
            .unwrap()
            .0
            .pop()
            .unwrap();
        timestamps.push(timestamp(format!("iter_{}_broadcast_err", iter)));

        iter += 1;

        // Reset sum vector for the next iteration
        for val in &mut sum {
            *val = 0.0;
        }
        let iter_t1 = SystemTime::now();
        let iter_t = iter_t1.duration_since(iter_t0).unwrap().as_secs_f32();

        println!(
            "[Worker {}] *** Completed iteration {}, took {:.2} s, err is {} ***",
            worker, iter, iter_t, norm
        );

        timestamps.push(timestamp(format!("iter_{}_end", iter)));
    }

    println!("[Worker {}] Finished", worker);
    timestamps.push(timestamp("worker_end".to_string()));
    Output {
        bucket: params.input_data.bucket.clone(),
        key: params.input_data.key.clone(),
        timestamps,
    }
}

// ow_main would be the entry point of an actual open whisk burst worker
pub fn main(args: Value, burst_middleware: Middleware<PagerankMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    let burst_middleware = burst_middleware.get_actor_handle();
    println!(
        "[Worker {}] Starting pagerank: {:?}",
        burst_middleware.info.worker_id, input
    );

    let result = pagerank(input, &burst_middleware);

    println!(
        "[Worker {}] Done: {:?}",
        burst_middleware.info.worker_id, result
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
