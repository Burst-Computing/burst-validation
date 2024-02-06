use mpi::collective::SystemOperation;
#[cfg(feature = "user-operations")]
use mpi::collective::UserOperation;
use mpi::topology::Rank;
use mpi::traits::*;

use nalgebra::DMatrix;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;

use rand::{rngs::StdRng, Rng, SeedableRng};

use kmeans::utils::{compute_clusters, get_timer, read_csv};

use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let root_rank = 0;
    let root_process = world.process_at_rank(root_rank);

    let _threshold = 0.00001;
    let num_dimensions = 2;
    let num_clusters = 4;
    let max_iterations = 100;

    let data_points = 200;

    let mut rng = StdRng::seed_from_u64(33);

    let mut communication: Duration = Default::default();

    let start_total = get_timer();
    println!("Start total: {:?}", start_total);

    // START GLOBAL_CENTROIDS
    println!(
        "Initializating Global Centroids with {} clusters and {} dimensions",
        num_clusters, num_dimensions
    );

    let mut correct_centroids = vec![0.0; num_clusters * num_dimensions];
    if world.rank() == root_rank {
        for k in 0..num_clusters {
            for d in 0..num_dimensions {
                correct_centroids[(k * num_dimensions) + d] = rng.gen_range(0.0..100.0);
            }
        }
    }

    let partition_points = data_points;
    let start_partition = 0;

    //Load CSV
    let file_name = format!("dataset/Mall_Customers.csv");

    let file = File::open(file_name).unwrap();
    let data: DMatrix<f32> = read_csv(&mut BufReader::new(file)).unwrap();

    let mut local_partition = data
        .rows(
            start_partition.try_into().unwrap(),
            partition_points.try_into().unwrap(),
        )
        .into_owned();
    let mut local_membership = vec![9999; local_partition.nrows()];

    local_partition = local_partition.transpose();
    let local_partition = local_partition.data.as_vec();

    let num_points = local_partition.len() / num_dimensions;

    println!("Number of Points: {:?}", num_points);

    let mut iter_count = 0;
    let mut global_delta_val = 10.0;
    //while iter_count < max_iterations && global_delta_val > threshold {

    while iter_count < max_iterations {

        // Get Centroids
        let start = get_timer();

        root_process.broadcast_into(&mut correct_centroids);

        let end = get_timer();

        communication += end - start;
        //println!("Rank {} correct_centroids: {:?}.", world.rank(), correct_centroids);

        // Reset local values
        let mut local_sizes = vec![0; num_clusters];
        let mut local_centroids = vec![0.0; num_clusters * num_dimensions];

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
        if world.rank() == root_rank {
            let mut global_delta: Rank = 0;
            let mut global_points: Rank = 0;

            let start = get_timer();

            root_process.reduce_into_root(&delta, &mut global_delta, SystemOperation::sum());
            root_process.reduce_into_root(&num_points, &mut global_points, SystemOperation::sum());

            let end = get_timer();

            communication += end - start;

            global_delta_val = global_delta as f32 / global_points as f32;
        } else {
            let start = get_timer();

            root_process.reduce_into(&delta, SystemOperation::sum());
            root_process.reduce_into(&num_points, SystemOperation::sum());

            let end = get_timer();

            communication += end - start;
        }

        //Update Centroids
        if world.rank() == root_rank {
            let mut all_centroids: Vec<f32> =
                vec![
                    0.0;
                    (world.size() * num_clusters as i32 * num_dimensions as i32)
                        .try_into()
                        .unwrap()
                ];

            let start = get_timer();

            //println!("rank 0: local_centroids: {:?}", local_centroids);
            root_process.gather_into_root(&local_centroids, &mut all_centroids);

            let end = get_timer();

            communication += end - start;

            let mut sum_centroids = vec![
                0.0;
                (num_clusters as i32 * num_dimensions as i32)
                    .try_into()
                    .unwrap()
            ];
            let mut i = 0;

            for centroid in &all_centroids {
                if i >= num_clusters * num_dimensions {
                    i = 0;
                }

                sum_centroids[i] += centroid;
                i += 1;
            }

            let mut all_sizes = vec![0; (world.size() * num_clusters as i32).try_into().unwrap()];

            let start = get_timer();

            root_process.gather_into_root(&local_sizes, &mut all_sizes);

            let end = get_timer();

            communication += end - start;

            let mut sum_sizes = vec![0; num_clusters];
            let mut i = 0;

            for size in &all_sizes {
                if i >= num_clusters {
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
                        correct_centroids[i_centroid + i] =
                            sum_centroids[i_centroid + i] as f32 / sum_sizes[i_sizes] as f32;
                    } else {
                        correct_centroids[i_centroid + i] = 0.0;
                    }
                }

                i_centroid += num_dimensions;
                i_sizes += 1;
            }
        } else {
            let start = get_timer();

            root_process.gather_into(&local_centroids);
            root_process.gather_into(&local_sizes);

            let end = get_timer();

            communication += end - start;
        }

        // Update Global_delta_val
        let start = get_timer();

        root_process.broadcast_into(&mut global_delta_val);

        let end = get_timer();

        communication += end - start;

        iter_count += 1;
    }

    //println!("Start_partition: {:?}, Cluster: {:?}", start_partition, local_membership);

    let end_total = get_timer();

    let total_time = end_total - start_total;

    if world.rank() == root_rank {
        println!("Correct centroids: {:?}", correct_centroids);
        println!("Time Master: {:?}", total_time);
        println!("Communication Master: {:?}", communication);
        println!("Compute Master: {:?}", total_time - communication);
    }

    Ok(())
}
