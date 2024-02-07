use tokio::runtime::Runtime;

use burst_communication_middleware::{MiddlewareActorHandle};
use burst_communication_middleware::Backend::Rabbitmq;

use std::collections::{HashMap, HashSet};
use std::thread;
use burst_communication_middleware::{create_actors, Config};
use kmeans::{Input, S3Config};

use kmeans::kmeans_burst;

const BURST_SIZE: u32 = 1;
const GROUPS: u32 = 1;

pub fn main() {
    env_logger::init();

    let s3_config = S3Config {
        region: String::from(""),
        aws_access_key_id: "".to_string(),
        aws_secret_access_key: "".to_string(),
        aws_session_token: "".to_string(),
    };

    let args = Input {
        bucket: String::from(""),
        key: String::from(""),
        s3_config: s3_config,
        threshold: 0.00001,
        num_dimensions: 2,
        num_clusters: 4,
        max_iterations: 100,
    };

    if BURST_SIZE % GROUPS != 0 {
        panic!("BURST_SIZE must be divisible by GROPUS");
    }

    let group_size = BURST_SIZE / GROUPS;

    let group_ranges: HashMap<String, HashSet<u32>> = (0..GROUPS)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((group_size * group_id)..((group_size * group_id) + group_size)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    for group_id in 0..GROUPS {
        let args_clone = args.clone();

        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        group(group_id, group_ranges.clone(), args_clone, runtime);
    }
}

fn group(
    group_id: u32,
    group_ranges: HashMap<String, HashSet<u32>>,
    args: Input,
    runtime: Runtime,
) {

    let binding = group_ranges.clone();
    let group_range = binding.get(&group_id.to_string()).unwrap();

    let mut actors = create_actors(
        Config {
            backend: Rabbitmq,
            server: Some("amqp://guest:guest@localhost:5672".to_string()),
            burst_id: "kmeans".to_string(), 
            burst_size: BURST_SIZE as u32,
            group_ranges,
            group_id: group_id.to_string(),
            chunking: true,
            // chunk_size received is in KB
            chunk_size: 1024*1024,
            tokio_broadcast_channel_size: Some(1024*1024),
        },
        &runtime,
    ).unwrap();

    // Create threads
    let mut handlers = Vec::new();
    
    
    for id in group_range.into_iter() {
        let id_clone = id.clone();
        let args_clone = args.clone();
        let actor = actors.remove(&id_clone).expect(format!("Error getting actor for id: {}", id_clone).as_str());
        handlers.push(thread::spawn(move || {
            println!("worker_id: {}", id_clone);
            println!("input: {:?}", args_clone);
            worker(args_clone, actor).unwrap()
        }));
    }

    for handle in handlers {
        let result = handle.join().expect("Error joining thread");
        println!("Results: {:?}", result);
    }
}

pub fn worker(
    args: Input,
    burst_middleware: MiddlewareActorHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = kmeans_burst(args, burst_middleware);

    println!("Done");
    println!("{:?}", result);

    Ok(())
}
