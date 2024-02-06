use nalgebra::DMatrix;
use std::error::Error;
use std::io::BufRead;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub fn read_csv(input: &mut dyn BufRead) -> Result<DMatrix<f32>, Box<dyn Error>> {
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

pub fn compute_clusters(
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

pub fn get_timer() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}
