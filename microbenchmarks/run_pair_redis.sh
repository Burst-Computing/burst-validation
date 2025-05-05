#!/usr/bin/env bash

set -xe

worker_type=c7i.large

redis_type=c7i.16xlarge

ubuntu_ami=""

docker_ami=""

subnet_id=""

ssh_access_sg_id=""
redis_sg_id=""

keypair=""

user="ubuntu"

# arguments: num_pairs, payload_size (MB), chunk_size (KB), repeats
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "Usage: $0 num_pairs payload_size(MB) chunk_size(KB) repeats"
    exit 1
fi

# burst configuration
num_pairs=$1
payload_size_mb=$2
chunk_size_kb=$3
repeats=$4

payload_size=$((payload_size_mb * 1024 * 1024))
chunk_size=$((chunk_size_kb * 1024))
burst_size=$((num_pairs * 2))
groups=2

stop() {
    # terminate instances
    terminate_instances $redis_instance_id ${worker_group_instance_ids[@]}
}

trap 'stop' ERR INT TERM

# arguments: image_id, instance_type, security_group_id, name, user_data
create_instance() {
    instance_id="$(aws ec2 run-instances \
        --image-id "$1" \
        --instance-type "$2" \
        --subnet-id "$subnet_id" \
        --security-group-ids "$ssh_access_sg_id" "$3" \
        --key-name "$keypair" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$4}]" \
        --user-data "$5" \
        --count 1 \
        --query "Instances[0].InstanceId" \
        --output text)"
        echo "$instance_id"
}

# arguments: image_id, instance_type, security_group_id, name, user_data
create_spot() {
    instance_id="$(aws ec2 run-instances \
        --image-id "$1" \
        --instance-type "$2" \
        --subnet-id "$subnet_id" \
        --security-group-ids "$ssh_access_sg_id" "$3" \
        --key-name "$keypair" \
        --instance-market-options MarketType=spot \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$4}]" \
        --user-data "$5" \
        --count 1 \
        --query "Instances[0].InstanceId" \
        --output text)"
        echo "$instance_id"
}

get_public_ip() {
    public_ip="$(aws ec2 describe-instances \
        --instance-ids "$1" \
        --query "Reservations[0].Instances[0].PublicIpAddress" \
        --output text)"
    echo "$public_ip"
}

get_internal_ip() {
    internal_ip="$(aws ec2 describe-instances \
        --instance-ids "$1" \
        --query "Reservations[0].Instances[0].PrivateIpAddress" \
        --output text)"
    echo "$internal_ip"
}

wait_for_instances() {
    aws ec2 wait instance-running --instance-ids $@
}

terminate_instances() {
    aws ec2 terminate-instances --instance-ids $@ --no-cli-pager
}

stop_instances() {
    aws ec2 stop-instances --instance-ids $@ --no-cli-pager
}

# arguments: local_path, ip, remote_path
put_file() {
    scp -o StrictHostKeyChecking=no -i "$keypair".pem "$1" "$user"@"$2":"$3"
}

# arguments: ip, remote_path, local_path
get_file() {
    scp -o StrictHostKeyChecking=no -i "$keypair".pem "$user"@"$1":"$2" "$3"
}

# arguments: ip, command
execute_command() {
    ssh -o StrictHostKeyChecking=no -i "$keypair".pem "$user"@"$1" "$2"
}

# create redis instance
redis_instance_id="$(create_spot "$docker_ami" "$redis_type" "$redis_sg_id" "redis")"

# create worker groups instances
worker_group_instance_ids=()
for group in $(seq 0 $((groups - 1))); do
    worker_group_id="worker_group$group"
    worker_group_instance_id="$(create_instance "$ubuntu_ami" "$worker_type" "$ssh_access_sg_id" "$worker_group_id")"
    worker_group_instance_ids+=("$worker_group_instance_id")
done

wait_for_instances $redis_instance_id ${worker_group_instance_ids[@]}
sleep 30

# get public ips
worker_group_ips=()
for worker_group_instance_id in ${worker_group_instance_ids[@]}; do
    worker_group_ip="$(get_public_ip $worker_group_instance_id)"
    worker_group_ips+=("$worker_group_ip")
done

# get redis public ip
redis_public="$(get_public_ip $redis_instance_id)"

# run redis container
execute_command "$redis_public" 'docker run -d --rm --name redis --network host --ulimit memlock=-1 docker.dragonflydb.io/dragonflydb/dragonfly --dbfilename ""'

# upload benchmark binary to worker groups
for worker_group_ip in ${worker_group_ips[@]}; do
    put_file target/release/benchmark "$worker_group_ip" benchmark &
done
wait

# get redis internal ip
redis_internal="$(get_internal_ip $redis_instance_id)"

# run benchmark
for repeat in $(seq 1 $repeats); do
    for i in $(seq 0 $((groups - 1))); do
        worker_group_ip="${worker_group_ips[$i]}"
        command="RUST_LOG=info ./benchmark --benchmark pair --burst-size $burst_size --groups $groups --burst-id pairs-$num_pairs-run-$repeat \
            --group-id $i --server redis://$redis_internal:6379 --payload-size $payload_size --chunking --chunk-size \
            $chunk_size redis-list"
        execute_command "$worker_group_ip" "$command" &
    done
    wait
    # flush redis
    execute_command "$redis_public" "docker exec redis redis-cli flushall"
    sleep 5
done

# compress results and copy them to local machine
for worker_group_ip in ${worker_group_ips[@]}; do
    execute_command "$worker_group_ip" "tar -czf results.tar.gz results" &
done
wait

for i in $(seq 0 $((groups - 1))); do
    worker_group_ip="${worker_group_ips[$i]}"
    get_file "$worker_group_ip" results.tar.gz results-$num_pairs-pairs-$payload_size_mb-MBpaylaod-$chunk_size_kb-KBchunk-$repeats-repeats-group-$i.tar.gz &
done
wait

stop
