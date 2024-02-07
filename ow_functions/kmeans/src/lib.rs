use nalgebra::DMatrix;
use std::io::BufRead;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions,
    TokioChannelImpl, TokioChannelOptions,
};

use bytes::Bytes;
use std::io::BufReader;

use rand::{rngs::StdRng, Rng, SeedableRng};

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_credential_types::Credentials;

use s3reader::S3ObjectUri;
use s3reader::S3Reader;

use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

use std::error::Error as stdError;

use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::thread;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Input {
    pub bucket: String,
    pub key: String,
    pub s3_config: S3Config,
    pub threshold: f32,
    pub num_dimensions: u32,
    pub num_clusters: u32,
    pub max_iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Config {
    pub region: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub aws_session_token: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Output {
    worker_id: u32,
    correct_centroids: Vec<f32>,
    communication_time: Duration,
    compute_time: Duration,
    total_time: Duration,
}

async fn get_matrix_from_s3(args: &Input) -> Result<DMatrix<f32>, Box<dyn stdError>> {
    let s3_uri = "s3://".to_owned() + &args.bucket + "/" + &args.key;
    println!("S3 Uri: {:?}", s3_uri);

    let credentials_provider = Credentials::from_keys(
        args.s3_config.aws_access_key_id.clone(),
        args.s3_config.aws_secret_access_key.clone(),
        Some(args.s3_config.aws_session_token.clone()),
    );

    let region_provider =
        RegionProviderChain::first_try(Region::new(args.s3_config.region.clone()));

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let uri = S3ObjectUri::new(&s3_uri).unwrap();
    let s3obj = S3Reader::from_config(&config, uri);

    Ok(read_csv(&mut BufReader::new(s3obj)).unwrap())
}

fn read_csv(input: &mut dyn BufRead) -> Result<DMatrix<f32>, Box<dyn stdError>> {
    let mut samples = Vec::new();

    let mut rows = 0;

    for line in input.lines() {
        rows += 1;

        for data in line?.split_terminator(",") {
            let a = f32::from_str(data.trim());

            match a {
                Ok(value) => samples.push(value),
                Err(_e) => println!("Error parsing data in row: {}", rows),
            }
        }
    }

    let cols = samples.len() / rows;

    Ok(DMatrix::from_row_slice(rows, cols, &samples[..]))
}

fn compute_clusters(
    local_centroids: &mut Vec<f32>,
    num_dimensions: usize,
    num_clusters: usize,
    local_partition: &Vec<f32>,
    correct_centroids: &Vec<f32>,
    local_sizes: &mut Vec<i32>,
    local_membership: &mut Vec<i32>,
) -> i32 {
    let mut delta = 0;
    let mut start = 0;

    let end = local_partition.len();
    while start < end {
        let mut point = Vec::new();
        for i in 0..num_dimensions {
            point.push(local_partition[start + i]);
        }

        let cluster =
            find_nearest_cluster(&point, num_clusters, &correct_centroids, num_dimensions);

        for i in 0..num_dimensions {
            local_centroids[((cluster * num_dimensions as i32) + i as i32) as usize] += point[i];
        }

        local_sizes[cluster as usize] += 1;

        if local_membership[start / num_dimensions] != cluster {
            delta += 1;
            local_membership[start / num_dimensions] = cluster;
        }

        start += num_dimensions;
    }

    delta
}

fn find_nearest_cluster(
    point: &Vec<f32>,
    num_clusters: usize,
    correct_centroids: &Vec<f32>,
    num_dimensions: usize,
) -> i32 {
    let mut cluster = 0;
    let mut min = 999999999999.0;

    let mut start = 0;
    let end = num_clusters * num_dimensions;
    while start < end {
        let mut centroid = Vec::new();
        for i in 0..num_dimensions {
            centroid.push(correct_centroids[start + i]);
        }

        let distance = distance(&point, centroid, num_dimensions);

        if distance < min {
            min = distance;
            cluster = start / num_dimensions;
        }

        start += num_dimensions;
    }

    cluster.try_into().unwrap()
}

fn distance(p: &Vec<f32>, centroid: Vec<f32>, num_dimensions: usize) -> f32 {
    let mut distance = 0.0;

    for i in 0..num_dimensions {
        distance += (p[i] - centroid[i]) * (p[i] - centroid[i]);
    }

    distance
}

fn get_timer() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

pub fn kmeans_burst(args: Input, burst_middleware: MiddlewareActorHandle) -> Output {
    let start_total = get_timer();

    let data_points = 200;

    let mut rng = StdRng::seed_from_u64(33);

    let mut communication: Duration = Default::default();

    // START GLOBAL_CENTROIDS
    println!(
        "Initializating Global Centroids with {} clusters and {} dimensions",
        args.num_clusters, args.num_dimensions
    );

    let mut correct_centroids = vec![
        0.0;
        (args.num_clusters * args.num_dimensions)
            .try_into()
            .unwrap()
    ];
    if burst_middleware.info.worker_id == 0 {
        for k in 0..args.num_clusters {
            for d in 0..args.num_dimensions {
                correct_centroids[((k * args.num_dimensions) + d) as usize] =
                    rng.gen_range(0.0..100.0);
            }
        }
    }

    let partition_points = data_points;
    let start_partition = 0;

    println!("Start: {:?}", start_partition);
    println!("Partition: {:?}", partition_points);

    //Load CSV
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let task = tokio_runtime.block_on(get_matrix_from_s3(&args));

    let data = task.unwrap();

    let mut local_partition = data
        .rows(
            start_partition.try_into().unwrap(),
            partition_points.try_into().unwrap(),
        )
        .into_owned();

    let mut local_membership = vec![9999; local_partition.nrows()];

    local_partition = local_partition.transpose();
    let local_partition = local_partition.data.as_vec();

    let num_points = local_partition.len() / args.num_dimensions as usize;

    println!("Number of Points: {:?}", num_points);

    let mut iter_count = 0;
    let mut global_delta_val = 10.0;
    //while iter_count < max_iterations && global_delta_val > threshold {
    while iter_count < args.max_iterations {
        // Get Centroids
        let res: Message;
        if burst_middleware.info.worker_id == 0 {
            let cc_bytes = unsafe {
                std::slice::from_raw_parts(
                    correct_centroids.as_ptr() as *const u8,
                    correct_centroids.len() * std::mem::size_of::<f32>(),
                )
            };

            let data = Bytes::from_static(cc_bytes);

            let start = get_timer();

            res = burst_middleware.broadcast(Some(data)).unwrap();

            let end = get_timer();

            communication += end - start;

            // Convert bytes to Vec<f32>
            let data = res.data.as_ref();
            let len = data.len();
            let ptr = data.as_ptr() as *const f32;
            correct_centroids = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
        } else {
            let start = get_timer();

            res = burst_middleware.broadcast(None).unwrap();

            let end = get_timer();

            communication += end - start;

            let data = res.data.as_ref();
            let len = data.len();
            let ptr = data.as_ptr() as *const f32;
            correct_centroids = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
        }

        // Reset local values
        let mut local_sizes = vec![0; args.num_clusters.try_into().unwrap()];
        let mut local_centroids = vec![
            0.0;
            (args.num_clusters * args.num_dimensions)
                .try_into()
                .unwrap()
        ];

        // Compute phase
        let delta = compute_clusters(
            &mut local_centroids,
            args.num_dimensions.try_into().unwrap(),
            args.num_clusters.try_into().unwrap(),
            local_partition,
            &correct_centroids,
            &mut local_sizes,
            &mut local_membership,
        );

        // Calculate delta
        let mut res_gather: Vec<Message>;
        if burst_middleware.info.worker_id == 0 {
            let data = Bytes::from(i32::to_le_bytes(delta).to_vec());

            let start = get_timer();

            res_gather = burst_middleware.gather(data).unwrap().unwrap();

            let end = get_timer();

            communication += end - start;

            let mut global_delta = 0;
            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const i32;
                let decoded = *unsafe { std::slice::from_raw_parts(ptr, len / 4) }
                    .to_vec()
                    .get(0)
                    .unwrap();
                global_delta += decoded;
            }

            let data = Bytes::from(i32::to_le_bytes(num_points.try_into().unwrap()).to_vec());

            let start = get_timer();

            res_gather = burst_middleware.gather(data).unwrap().unwrap();

            let end = get_timer();

            communication += end - start;

            let mut global_points = 0;

            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const i32;
                let decoded = *unsafe { std::slice::from_raw_parts(ptr, len / 4) }
                    .to_vec()
                    .get(0)
                    .unwrap();
                global_points += decoded;
            }

            global_delta_val = global_delta as f32 / global_points as f32;
        } else {
            let data = Bytes::from(i32::to_le_bytes(delta).to_vec());

            let start = get_timer();

            burst_middleware.gather(data).unwrap();

            let end = get_timer();

            communication += end - start;

            let data = Bytes::from(i32::to_le_bytes(num_points.try_into().unwrap()).to_vec());

            let start = get_timer();

            burst_middleware.gather(data).unwrap();

            let end = get_timer();
            communication += end - start;
        }

        // Update Centroids
        let mut res_gather: Vec<Message>;
        if burst_middleware.info.worker_id == 0 {
            let lc_bytes = unsafe {
                std::slice::from_raw_parts(
                    local_centroids.as_ptr() as *const u8,
                    local_centroids.len() * std::mem::size_of::<f32>(),
                )
            };

            let data = Bytes::from_static(lc_bytes);

            let start = get_timer();

            res_gather = burst_middleware.gather(data).unwrap().unwrap();

            let end = get_timer();

            communication += end - start;

            let capacity =
                burst_middleware.info.burst_size as u32 * args.num_clusters * args.num_dimensions;
            let mut all_centroids = vec![0.0; capacity.try_into().unwrap()];

            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const f32;
                all_centroids = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
            }

            let mut sum_centroids = vec![
                0.0;
                (args.num_clusters as u32 * args.num_dimensions)
                    .try_into()
                    .unwrap()
            ];
            let mut i = 0;

            for centroid in &all_centroids {
                if i >= (args.num_clusters * args.num_dimensions)
                    .try_into()
                    .unwrap()
                {
                    i = 0;
                }

                sum_centroids[i] += centroid;
                i += 1;
            }

            let ls_bytes = unsafe {
                std::slice::from_raw_parts(
                    local_sizes.as_ptr() as *const u8,
                    local_sizes.len() * std::mem::size_of::<i32>(),
                )
            };

            let data = Bytes::from_static(ls_bytes);

            let start = get_timer();

            res_gather = burst_middleware.gather(data).unwrap().unwrap();

            let end = get_timer();

            communication += end - start;

            let capacity = burst_middleware.info.burst_size as u32 * args.num_clusters;
            let mut all_sizes = vec![0; capacity.try_into().unwrap()];

            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const u32;
                all_sizes = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
            }

            let mut sum_sizes = vec![0; args.num_clusters.try_into().unwrap()];
            i = 0;

            for size in &all_sizes {
                if i >= args.num_clusters.try_into().unwrap() {
                    i = 0;
                }

                sum_sizes[i] += size;
                i += 1;
            }

            let mut i_centroid = 0;
            let mut i_sizes = 0;

            while i_centroid < sum_centroids.len() {
                for i in 0..args.num_dimensions {
                    if sum_sizes[i_sizes] != 0 {
                        correct_centroids[i_centroid + i as usize] =
                            sum_centroids[i_centroid + i as usize] as f32
                                / sum_sizes[i_sizes] as f32;
                    } else {
                        correct_centroids[i_centroid + i as usize] = 0.0;
                    }
                }

                i_centroid += args.num_dimensions as usize;
                i_sizes += 1;
            }
        } else {
            let lc_bytes = unsafe {
                std::slice::from_raw_parts(
                    local_centroids.as_ptr() as *const u8,
                    local_centroids.len() * std::mem::size_of::<f32>(),
                )
            };

            let data = Bytes::from_static(lc_bytes);

            let start = get_timer();

            burst_middleware.gather(data).unwrap();

            let end = get_timer();

            communication += end - start;

            let ls_bytes = unsafe {
                std::slice::from_raw_parts(
                    local_sizes.as_ptr() as *const u8,
                    local_sizes.len() * std::mem::size_of::<i32>(),
                )
            };

            let data = Bytes::from(ls_bytes);

            let start = get_timer();

            burst_middleware.gather(data).unwrap();

            let end = get_timer();

            communication += end - start;
        }

        // Update global delta val
        let res: Message;
        if burst_middleware.info.worker_id == 0 {
            let data = Bytes::from(f32::to_le_bytes(global_delta_val.try_into().unwrap()).to_vec());

            let start = get_timer();

            res = burst_middleware.broadcast(Some(data)).unwrap();

            let end = get_timer();

            communication += end - start;

            let data = res.data.as_ref();
            let len = data.len();
            let ptr = data.as_ptr() as *const f32;
            global_delta_val = *unsafe { std::slice::from_raw_parts(ptr, len / 4) }
                .to_vec()
                .get(0)
                .unwrap();
        } else {
            let start = get_timer();

            res = burst_middleware.broadcast(None).unwrap();

            let end = get_timer();

            communication += end - start;

            let data = res.data.as_ref();
            let len = data.len();
            let ptr = data.as_ptr() as *const f32;
            global_delta_val = *unsafe { std::slice::from_raw_parts(ptr, len / 4) }
                .to_vec()
                .get(0)
                .unwrap();
        }

        iter_count += 1;
    }

    println!("iter: {:?}", iter_count);

    //println!("Start_partition: {:?}, Cluster: {:?}", start_partition, local_membership);

    let end_total = get_timer();

    let total_time = end_total - start_total;

    Output {
        worker_id: burst_middleware.info.worker_id,
        correct_centroids: correct_centroids,
        communication_time: communication,
        compute_time: total_time - communication,
        total_time: total_time,
    }
}

// ow_main would be the entry point of an actual open whisk burst worker
/* pub fn main(args: Value, burst_middleware: MiddlewareActorHandle) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    println!("Starting kmeans: {:?}", input);

    let result = kmeans_burst(input, burst_middleware);

    println!("Done");
    println!("{:?}", result);
    serde_json::to_value(result)
} */

// main function used for debugging
// To do