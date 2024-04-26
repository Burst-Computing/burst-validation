import argparse
import json

import boto3

from pprint import pprint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate input for the Hypertuning application on OpenWhisk Burst")
    parser.add_argument("--workers", type=int, required=True,
                        help="Number of workers to use")
    parser.add_argument("--granularity", type=int, required=True,
                        help="Granularity of burst workers")
    parser.add_argument("--s3_bucket", type=str,
                        required=True, help="S3 bucket name")
    parser.add_argument("--s3_key", type=str,
                        required=True, help="S3 key prefix")
    parser.add_argument("--s3_endpoint", type=str,
                        default="", help="S3 endpoint URL")
    parser.add_argument("--s3_region", type=str,
                        required=True, help="S3 region name")
    parser.add_argument("--aws_access_key_id", type=str,
                        required=True, help="AWS access key ID")
    parser.add_argument("--aws_secret_access_key", type=str,
                        required=True, help="AWS secret access key")
    parser.add_argument(
        "--output", type=str, default="hypertuning_payload.json", help="Output file path")
    parser.add_argument("--split", type=int, default=1,
                        help="Split output file")
    parser.add_argument("--mib", type=int, required=False, default=None,
                        help="Load X MiB from the dataset")
    args = parser.parse_args()

    pprint(args)

    base_payload = {
        "bucket": args.s3_bucket,
        "key": args.s3_key,
        "s3_config": {
            "region": args.s3_region,
            "endpoint": args.s3_endpoint,
            "aws_access_key_id": args.aws_access_key_id,
            "aws_secret_access_key": args.aws_secret_access_key,
        },
        "start_byte": None,
        "end_byte": None,
        "base_worker_id": None,
        "granularity": args.granularity,
    }

    if args.mib:
        base_payload["mib"] = args.mib

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
    
    if args.split == 1:
        with open(args.output, "w") as f:
            f.write(json.dumps(params, indent=4))
    else:
        assert args.partitions % args.split == 0
        sz = args.partitions // args.split
        for i in range(args.split):
            with open(f"part-{str(i).zfill(4)}" + args.output, "w") as f:
                f.write(json.dumps(
                    params[sz * i:(sz * i) + sz], indent=4))
