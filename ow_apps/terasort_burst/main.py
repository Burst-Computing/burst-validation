import argparse

from ow_apps.helpers.parser import add_openwhisk_to_parser, add_terasort_to_parser, add_burst_to_parser, try_or_except
from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_apps.helpers.terasort_helper import generate_payload, complete_mpu

# PRECONDITION: This use case needs to have stored terasort file in Minio

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_terasort_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.ts_endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port)
    dt = executor.burst("terasort-burst",
                        params,
                        memory=args.runtime_memory if args.runtime_memory else 4096,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        burst_size=args.granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True)

    flattened_results = [item for sublist in dt.get_results() for item in sublist]
    flattened_results.sort(key=lambda x: x['part_number'])

    complete_mpu(endpoint=args.ts_endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
                 mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in flattened_results]})
