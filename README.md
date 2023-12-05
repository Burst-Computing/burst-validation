# burst-validation

## Description

This repository contains the validation code for the [burst commuinication middleware](https://github.com/CLOUDLAB-URV/burst-communication-middleware).

## Benchmark

The benchmark used for the validation is in the [benchmark](benchmark) folder.

### Running the benchmark

```bash
$ cd benchmark
$ cargo run --release -- --help

[...]

Usage: benchmark [OPTIONS] --benchmark <BENCHMARK> --burst-size <BURST_SIZE> --group-id <GROUP_ID> <COMMAND>

Commands:
  s3             Use S3 as backend
  redis          Use Redis as backend
  rabbitmq       Use RabbitMQ as backend
  message-relay  Use burst message relay as backend
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

      --repeat <REPEAT>
          Repeat count
          
          [default: 256]

      --tokio-broadcast-channel-size <TOKIO_BROADCAST_CHANNEL_SIZE>
          Tokio broadcast channel size
          
          [default: 1048576]

  -h, --help
          Print help (see a summary with '-h')
```

### Example

Pair benchmark with 2 workers and 2 groups using RabbitMQ as backend:

```bash
$ cd benchmark
$ export RUST_LOG=info
$ cargo run --release -- --benchmark pair --burst-size 2 --group-id 0 --server "amqp://guest:guest@localhost:5672" rabbitmq & cargo run --release -- --benchmark pair --burst-size 2 --group-id 1 --server "amqp://guest:guest@localhost:5672" rabbitmq

[...]
```
