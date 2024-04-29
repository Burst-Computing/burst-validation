import argparse

import boto3

from ow_client.openwhisk_executor import OpenwhiskExecutor
from ow_client.parser import add_burst_to_parser, try_or_except, add_openwhisk_to_parser


def add_hypertuning_to_parser(parser):
    parser.add_argument("--workers", type=int, help="Number of workers to use", required=True)
    parser.add_argument("--python-script", type=str, help="Python script to run", required=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_hypertuning_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    base_payload = {
        "bucket": "hypertuning",
        "key": "train.ft.txt.bz2",
        "s3_config": {
            "region": "us-east-1",
            "endpoint": "http://192.168.5.24:9000",
            "aws_access_key_id": "lab144",
            "aws_secret_access_key": "astl1a4b4",
        },
        "start_byte": None,
        "end_byte": None,
        "base_worker_id": None,
        "granularity": args.granularity,
        "python_script": args.python_script,
    }

    s3_client = boto3.client(
        's3',
        region_name=base_payload["s3_config"]["region"],
        endpoint_url=base_payload["s3_config"]["endpoint"],
        aws_access_key_id=base_payload["s3_config"]["aws_access_key_id"],
        aws_secret_access_key=base_payload["s3_config"]["aws_secret_access_key"]
    )

    input_head = s3_client.head_object(Bucket=base_payload["bucket"], Key=base_payload["key"])

    params = []
    for i in range(args.workers):
        payload = base_payload.copy()
        payload["base_worker_id"] = (i // args.granularity) * args.granularity
        payload["start_byte"] = (input_head["ContentLength"] // args.granularity) * (i % args.granularity) + 1
        if i % args.granularity == 0:
            payload["start_byte"] = 0
        payload["end_byte"] = (input_head["ContentLength"] // args.granularity) * (i % args.granularity + 1)
        if i % args.granularity == args.granularity - 1:
            payload["end_byte"] = input_head["ContentLength"]
        params.append(payload)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)
    dt = executor.burst("hypertuning-burst",
                        params,
                        file="hypertuning/hypertuning.zip",
                        memory=args.runtime_memory,
                        burst_size=args.granularity,
                        debug_mode=args.debug,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        custom_image=args.custom_image,
                        is_zip=True)
    dt.plot()
    result = dt.get_results()
    print("*** HyperTuning finished ***")
