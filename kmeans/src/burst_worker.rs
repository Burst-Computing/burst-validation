use burst_communication_middleware::{
    MiddlewareActorHandle,
    Message
};

use bytes::Bytes;
use std::error::Error;
use std::io::BufReader;

use rand::{rngs::StdRng, Rng, SeedableRng};

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_credential_types::Credentials;

use s3reader::S3ObjectUri;
use s3reader::S3Reader;

use std::time::Duration;

use crate::utils::{S3Credentials, compute_clusters, get_timer, read_csv};


pub async fn burst_worker(
    s3_uri: String,
    s3_credentials: S3Credentials,
    burst_middleware: MiddlewareActorHandle,
    _threshold: f32,
    num_dimensions: i32,
    num_clusters: i32,
    max_iterations: i32,
) -> Result<(), Box<dyn Error>> {
    let start_total = get_timer();

    let data_points = 200;

    let mut rng = StdRng::seed_from_u64(33);

    let mut communication: Duration = Default::default();

    // START GLOBAL_CENTROIDS
    println!(
        "Initializating Global Centroids with {} clusters and {} dimensions",
        num_clusters, num_dimensions
    );

    let mut correct_centroids = vec![0.0; (num_clusters * num_dimensions).try_into().unwrap()];
    if burst_middleware.info.worker_id == 0 {
        for k in 0..num_clusters {
            for d in 0..num_dimensions {
                correct_centroids[((k * num_dimensions) + d) as usize] = rng.gen_range(0.0..100.0);
            }
        }

    }

    let partition_points = data_points;
    let start_partition = 0;

    println!("Start: {:?}", start_partition);
    println!("Partition: {:?}", partition_points);
    //Load CSV
    println!("S3 Uri: {:?}", s3_uri);

    let credentials_provider = Credentials::from_keys(
        s3_credentials.access_key_id,
        s3_credentials.secret_access_key,
        s3_credentials.session_token,
    );

    let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let task = tokio::task::spawn_blocking(move || {
        let uri = S3ObjectUri::new(&s3_uri).unwrap();
        let s3obj = S3Reader::from_config(&config, uri);
        read_csv(&mut BufReader::new(s3obj)).unwrap()
    })
    .await;

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

    let num_points = local_partition.len() / num_dimensions as usize;

    println!("Number of Points: {:?}", num_points);

    let mut iter_count = 0;
    let mut global_delta_val = 10.0;
    //while iter_count < max_iterations && global_delta_val > threshold {
    while iter_count < max_iterations {
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
        let mut local_sizes = vec![0; num_clusters.try_into().unwrap()];
        let mut local_centroids = vec![0.0; (num_clusters * num_dimensions).try_into().unwrap()];

        // Compute phase
        let delta = compute_clusters(
            &mut local_centroids,
            num_dimensions.try_into().unwrap(),
            num_clusters.try_into().unwrap(),
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
                burst_middleware.info.burst_size as i32 * num_clusters * num_dimensions;
            let mut all_centroids = vec![0.0; capacity.try_into().unwrap()];

            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const f32;
                all_centroids = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
            }

            let mut sum_centroids =
                vec![0.0; (num_clusters as i32 * num_dimensions).try_into().unwrap()];
            let mut i = 0;

            for centroid in &all_centroids {
                if i >= (num_clusters * num_dimensions).try_into().unwrap() {
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

            let capacity = burst_middleware.info.burst_size as i32 * num_clusters;
            let mut all_sizes = vec![0; capacity.try_into().unwrap()];

            for message in res_gather {
                let data = message.data.as_ref();
                let len = data.len();
                let ptr = data.as_ptr() as *const i32;
                all_sizes = unsafe { std::slice::from_raw_parts(ptr, len / 4) }.to_vec();
            }

            let mut sum_sizes = vec![0; num_clusters.try_into().unwrap()];
            i = 0;

            for size in &all_sizes {
                if i >= num_clusters.try_into().unwrap() {
                    i = 0;
                }

                sum_sizes[i] += size;
                i += 1;
            }

            let mut i_centroid = 0;
            let mut i_sizes = 0;

            while i_centroid < sum_centroids.len() {
                for i in 0..num_dimensions {
                    if sum_sizes[i_sizes] != 0 {
                        correct_centroids[i_centroid + i as usize] =
                            sum_centroids[i_centroid + i as usize] as f32
                                / sum_sizes[i_sizes] as f32;
                    } else {
                        correct_centroids[i_centroid + i as usize] = 0.0;
                    }
                }

                i_centroid += num_dimensions as usize;
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

    if burst_middleware.info.worker_id == 0 {
        println!("correct_centroids: {:?}", correct_centroids);
        println!("Time Master: {:?}", total_time);
        println!("Communication Master: {:?}", communication);
        println!("Compute Master: {:?}", total_time - communication);
    }

    Ok(())
}