import argparse

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_apps.terasort_classic.terasort_utils import generate_payload, complete_mpu

# PRECONDITION: This use case needs to have stored terasort file in Minio

# Ex. usage: python3 main.py --endpoint http://172.17.0.1:9000 --partitions 2 --bucket terasort --key terasort-250m
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, required=True, help="Endpoint of the S3 service")
    parser.add_argument("--partitions", type=int, required=True, help="Number of partitions")
    parser.add_argument("--bucket", type=str, required=True, help="Bucket name")
    parser.add_argument("--key", type=str, required=True, help="Object key")
    args = parser.parse_args()

    params = generate_payload(endpoint=args.endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    executor = OpenwhiskExecutor("172.17.0.1", 3233)
    dt = executor.burst("terasort-burst", params, memory=4096,
                        custom_image="manriurv/rust-burst:1.72.1", is_zip=True)

    flattened_results = [item for sublist in dt.get_results() for item in sublist]
    flattened_results.sort(key=lambda x: x['part_number'])

    complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
                 mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in flattened_results]})
