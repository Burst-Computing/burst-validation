import argparse
import json
import boto3
import os
from pprint import pprint

DEFAULT_ERROR = 0.00005
DEFAULT_OUTPUT = "pagerank_payload.json"

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "lab144")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "astl1a4b4")

def generate_payload(endpoint, partitions, num_nodes, bucket, key, error=DEFAULT_ERROR):
    payload = []
    for i in range(partitions):
        payload.append(
            {
                "num_nodes": num_nodes,
                "error": error,
                "input_data": {
                    "bucket": bucket,
                    "key": f"{key}/part-{str(i).zfill(5)}",
                    "endpoint": endpoint,
                    "region": AWS_S3_REGION,
                    "aws_access_key_id": AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                },
            }
        )

    return payload

def complete_mpu(endpoint, bucket, key, upload_id, mpu):
    s3_client = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_S3_REGION)
    result = s3_client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                                 MultipartUpload=mpu)
    # print(result)

def add_pagerank_to_parser(parser):
    parser.add_argument("--pr-endpoint", type=str, required=True,
                        help="Endpoint of the S3 service in which the pagerank file is stored")
    parser.add_argument("--partitions", type=int, required=True, help="Number of partitions")
    parser.add_argument("--num-nodes", type=int, required=True, help="Number of nodes in the dataset graph")
    parser.add_argument("--bucket", type=str, required=True, help="Pagerank bucket name")
    parser.add_argument("--key", type=str, required=True, help="Pagerank object key")
