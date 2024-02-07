import argparse

from ow_apps.helpers.parser import add_openwhisk_to_parser, add_terasort_to_parser, try_or_except
from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_apps.terasort_classic.terasort_utils import generate_payload, complete_mpu

# PRECONDITION: This use case needs to have stored terasort file in Minio

# Ex. usage: python3 main.py --endpoint http://172.17.0.1:9000 --partitions 2 --bucket terasort --key terasort-250m
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_terasort_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port)
    dt_map = executor.map("terasort-map", params,
                          memory=args.runtime_memory if args.runtime_memory else 256,
                          custom_image=args.custom_image, is_zip=True)

    dt_reduce = executor.map("terasort-reduce", params,
                             memory=args.runtime_memory if args.runtime_memory else 256,
                             custom_image=args.custom_image, is_zip=True)

    complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
                 mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in
                                dt_reduce.get_results().sort(key=lambda x: x['part_number'])]})
