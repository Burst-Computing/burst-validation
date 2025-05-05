#!/usr/bin/env bash

set -xe

worker_type=c7i.12xlarge

redis_type=c7i.48xlarge

ubuntu_ami=""

docker_ami=""

subnet_id=""
ssh_access_sg_id=""
redis_sg_id=""

keypair=""

user="ubuntu"

MB=$((1024 * 1024))
GB=$((1024 * 1024 * 1024))

# arguments: benchmark burst_size granularity repeats
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "Usage: $0 benchmark burst_size granularity repeats"
    exit 1
fi

# burst configuration
benchmark=$1
burst_size=$2
granularity=$3
repeats=$4


payload_size=$((4 * MB))
chunk_size=$((1 * MB))
backend="redis-list"

num_processes=$((burst_size / granularity))
num_vms=$((burst_size / 48))


stop() {
    # terminate instances
    terminate_instances $redis_instance_id ${vm_instance_ids[@]}
}

trap 'stop' ERR INT TERM

# arguments: image_id, instance_type, security_group_id, name, count, user_data
create_instances() {
    instance_ids="$(aws ec2 run-instances \
        --image-id "$1" \
        --instance-type "$2" \
        --subnet-id "$subnet_id" \
        --security-group-ids "$ssh_access_sg_id" "$3" \
        --key-name "$keypair" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$4}]" \
        --count "$5" \
        --user-data "$6" \
        --query "Instances[*].InstanceId" \
        --output text)"
        echo "$instance_ids"
}

# arguments: image_id, instance_type, security_group_id, name, count, user_data
create_spots() {
    instance_id="$(aws ec2 run-instances \
        --image-id "$1" \
        --instance-type "$2" \
        --subnet-id "$subnet_id" \
        --security-group-ids "$ssh_access_sg_id" "$3" \
        --key-name "$keypair" \
        --instance-market-options MarketType=spot \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$4}]" \
        --count "$5" \
        --user-data "$6" \
        --query "Instances[*].InstanceId" \
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

# create vms
vm_instance_ids="$(create_spots "$ubuntu_ami" "$worker_type" "$ssh_access_sg_id" "vm" "$num_vms")"

# create redis instance
redis_instance_id="$(create_spots "$docker_ami" "$redis_type" "$redis_sg_id" "redis" 1 )"

wait_for_instances $redis_instance_id ${vm_instance_ids[@]}
sleep 30

# get public ips
vm_ips=()
for vm in ${vm_instance_ids[@]}; do
    vm_ip="$(get_public_ip $vm)"
    vm_ips+=("$vm_ip")
done

# get redis public ip
redis_public="$(get_public_ip $redis_instance_id)"

execute_command $redis_public 'docker run -d --rm --name redis --network host --ulimit memlock=-1 docker.dragonflydb.io/dragonflydb/dragonfly --dbfilename ""'


# upload benchmark binary to vms
for vm_ip in ${vm_ips[@]}; do
    put_file target/release/benchmark "$vm_ip" benchmark &
    put_file run_benchmark.sh "$vm_ip" run_benchmark.sh &
done
wait

# get redis internal ip
redis_internal="$(get_internal_ip $redis_instance_id)"

# run benchmark
for repeat in $(seq 1 $repeats); do
    for i in $(seq 0 $((num_vms - 1))); do
        vm_ip="${vm_ips[$i]}"
        group_ini=$((i * num_processes/num_vms))
        group_end=$((group_ini + num_processes/num_vms - 1))
        command="./run_benchmark.sh $benchmark $burst_size $num_processes $group_ini $group_end \
            redis://$redis_internal:6379 $payload_size $chunk_size \
            $benchmark-bs-$burst_size-g-$granularity-run-$repeat $backend"
        execute_command "$vm_ip" "$command" &
    done
    wait
    execute_command $redis_public 'docker exec redis redis-cli flushall'
    sleep 5
done

# compress results and copy them to local machine
for vm_ip in ${vm_ips[@]}; do
    execute_command "$vm_ip" "tar -czf results.tar.gz results" &
done
wait

for i in $(seq 0 $((num_vms - 1))); do
    vm_ip="${vm_ips[$i]}"
    get_file "$vm_ip" results.tar.gz results-$benchmark-bs-$burst_size-g-$granularity-vm-$i.tar.gz &
done
wait

stop
