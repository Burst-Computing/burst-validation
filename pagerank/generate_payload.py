import argparse
import json
from pprint import pprint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate input for the Pagerank application on OpenWhisk Burst")
    parser.add_argument(
        "--partitions", type=int, required=True, help="Number of dataset partitions (burst size, num of workers)"
    )
    parser.add_argument("--num_nodes", type=int, required=True, help="Number of nodes in the dataset graph")
    parser.add_argument("--error", type=float, default=0.00005, help="Convergence error threshold")
    parser.add_argument("--s3_bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--s3_key", type=str, required=True, help="S3 key prefix")
    parser.add_argument("--s3_endpoint", type=str, default="", help="S3 endpoint URL")
    parser.add_argument("--s3_region", type=str, default="", help="S3 region name")
    parser.add_argument("--aws_access_key_id", type=str, required=True, help="AWS access key ID")
    parser.add_argument("--aws_secret_access_key", type=str, required=True, help="AWS secret access key")
    parser.add_argument("--output", type=str, default="pagerank_payload.json", help="Output file path")
    args = parser.parse_args()

    pprint(args)

    payload = []
    for i in range(args.partitions):
        payload.append(
            {
                "num_nodes": args.num_nodes,
                "error": args.error,
                "input_data": {
                    "bucket": args.s3_bucket,
                    "key": f"{args.s3_key}/part-{str(i).zfill(5)}",
                    "endpoint": args.s3_endpoint,
                    "region": args.s3_region,
                    "aws_access_key_id": args.aws_access_key_id,
                    "aws_secret_access_key": args.aws_secret_access_key,
                },
            }
        )

    with open(args.output, "w") as f:
        f.write(json.dumps(payload, indent=4))
