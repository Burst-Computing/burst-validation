# Microbenchmarks

This directory contains the microbenchmarks to test the performance of the indirect communication middleware with all the different communication patterns. These microbenchmarks are intended to be deployed directly on different hosts without any virtualization/isolation infrastructure (i.e. on bare metal). 

## Running the benchmark

```bash
$ cd benchmark
$ cargo run --release -- --help

[...]

Usage: benchmark [OPTIONS] --benchmark <BENCHMARK> --burst-size <BURST_SIZE> --group-id <GROUP_ID> <COMMAND>

Commands:
  s3             Use S3 as backend
  redis-stream   Use Redis Streams as backend
  redis-list     Use Redis Lists as backend
  rabbitmq       Use RabbitMQ as backend
  help           Print this message or the help of the given subcommand(s)

Options:
      --benchmark <BENCHMARK>
          Benchmark to run

          Possible values:
          - pair:       Run pair benchmark
          - broadcast:  Run broadcast benchmark
          - scatter:    Run scatter benchmark
          - gather:     Run gather benchmark
          - all-to-all: Run all-to-all benchmark

      --server <SERVER>
          Server address, URI or endpoint

      --burst-id <BURST_ID>
          Burst ID
          
          [default: burst]

      --burst-size <BURST_SIZE>
          Burst Size

      --groups <GROUPS>
          Groups
          
          [default: 2]

      --group-id <GROUP_ID>
          Group id

      --payload-size <PAYLOAD_SIZE>
          Payload size
          
          [default: 1048576]

      --chunking
          Enable chunking

      --chunk-size <CHUNK_SIZE>
          Chunk size
          
          [default: 1048576]

      --tokio-broadcast-channel-size <TOKIO_BROADCAST_CHANNEL_SIZE>
          Tokio broadcast channel size
          
          [default: 1048576]

  -h, --help
          Print help (see a summary with '-h')
```

## Example

Pair benchmark with 2 workers and 2 groups using RabbitMQ as backend:

```bash
$ cd benchmark
$ export RUST_LOG=info
$ cargo run --release -- --benchmark pair --burst-size 2 --group-id 0 --server "amqp://guest:guest@localhost:5672" rabbitmq & cargo run --release -- --benchmark pair --burst-size 2 --group-id 1 --server "amqp://guest:guest@localhost:5672" rabbitmq

[...]
```

## Execute in AWS

Prerequisites:

- AWS CLI with credentials configured
- AWS AMI with docker installed
- AWS VPC with a public subnet and one AZ
- SSH key pair
- 2 AWS security groups for the VPC:
  - One for ssh (port 22)
  - Another for the intermediate server (e.g. port 6379 for Redis)

### Message chunk size and maximum throughput benchmarks

The provided scripts `run_pair_redis.sh` and `run_pair_s3.sh` can be used to run the pair benchmark with Redis and S3 as backends, respectively. They can be modified to run the benchmark with other backends.

Before running the benchmark, you need to change the corresponding variables in the `run_pair_redis.sh` script:

- `worker_type`: The type of the EC2 instance to use for the workers (e.g. `c7i.large`).
- `redis_type`: The type of the EC2 instance to use for the Redis server (e.g. `c7i.16xlarge`).
- `ubuntu_ami`: The ID of the Ubuntu AMI.
- `docker_ami`: The AMI with Docker installed.
- `subnet_id`: The ID of the subnet.
- `ssh_access_sg_id`: The ID of the security group with SSH access.
- `redis_sg_id`: The ID of the security group for the Redis server.
- `keypair`: The name of the SSH key pair.

```bash
$ ./run_pair_redis.sh
Usage: ./run_pair_redis.sh num_pairs payload_size(MB) chunk_size(KB) repeats
```

For message chunk size benchmarks, you should set the `num_pairs` to 1, fix `payload_size`, and vary the `chunk_size` parameter.
For maximum throughput benchmarks, you should fix the `chunk_size` and `payload_size`, and vary the `num_pairs` parameter.

### Group collevtive benchmarks

Before running the collective benchmarks, you need to change the corresponding variables in the `run_bencharmk_redis.sh` script (same as above).

```bash
$ ./run_benchmark_redis.sh
Usage: ./run_benchmark_redis.sh benchmark burst_size granularity repeats
```

Where `benchmark` can be `broadcast`, or `all-to-all`.
