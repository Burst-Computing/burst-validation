#!/usr/bin/env bash

set -xe

if [ "$#" -ne 10 ]; then
    echo "Illegal number of parameters"
    echo "Usage: $0 benchmark burst_size groups group_ini group_end server payload_size chunk_size burst_id backend"
    exit 1
fi

benchmark=$1
burst_size=$2
groups=$3
group_ini=$4
group_end=$5
server=$6
payload_size=$7
chunk_size=$8
burst_id=$9
backend=${10}

for group in $(seq $group_ini $group_end); do
    cmd="./benchmark \
        --benchmark $benchmark \
        --burst-size $burst_size \
        --groups $groups \
        --group-id $group \
        --server $server \
        --payload-size $payload_size \
        --chunking \
        --chunk-size $chunk_size \
        --burst-id $burst_id \
        $backend"
    if [ -n "$RUST_LOG" ]; then
        RUST_LOG=$RUST_LOG $cmd &
    fi
    $cmd &
done
wait
