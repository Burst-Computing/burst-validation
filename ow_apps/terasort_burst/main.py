import argparse

from ow_apps.helpers.defs import VALID_BURST_BACKEND_OPTIONS
from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_apps.helpers.terasort_helper import generate_payload, complete_mpu

# PRECONDITION: This use case needs to have stored terasort file in Minio

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, required=True,
                        help="Endpoint of the S3 service in which the terasort file is stored")
    parser.add_argument("--partitions", type=int, required=True,
                        help="Number of partitions to sort the file into")
    parser.add_argument("--bucket", type=str, required=True,
                        help="Terasort bucket name")
    parser.add_argument("--key", type=str, required=True,
                        help="Terasort object key")
    parser.add_argument("--granularity", type=int, required=False, help="Granularity of burst workers",
                        default=None)
    parser.add_argument("--join", type=bool, required=False, help="Join burst workers in same invoker",
                        default=False)
    parser.add_argument("--backend", type=str, required=True, help="Burst communication backend",
                        choices=VALID_BURST_BACKEND_OPTIONS)
    parser.add_argument("--chunk_size", type=int, required=False, help="Chunk size for burst messages",
                        default=1)

    try:
        args = parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        exit(1)

    params = generate_payload(endpoint=args.endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    executor = OpenwhiskExecutor("172.17.0.1", 3233)
    dt = executor.burst("terasort-burst",
                        params,
                        memory=4096,
                        custom_image="manriurv/rust-burst:1.72.1",
                        burst_size=args.granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True)

    flattened_results = [item for sublist in dt.get_results() for item in sublist]
    flattened_results.sort(key=lambda x: x['part_number'])

    complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
                 mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in flattened_results]})
