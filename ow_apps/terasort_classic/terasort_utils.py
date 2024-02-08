import logging
from math import floor
import random
import json
import argparse
import boto3
import pandas as pd
import numpy as np
from pprint import pprint
from io import BytesIO

logger = logging.getLogger(__name__)

DEFAULT_MAX_SAMPLE_SIZE: int = 1 * 1024 * 1024
DEFAULT_SAMPLE_RATIO: float = 0.01
DEFAULT_SAMPLE_FRAGMENTS: int = 20
DEFAULT_START_MARGIN: float = 0.02
DEFAULT_END_MARGIN: float = 0.02
DEFUALT_BOUND_EXTRACTION_MARGIN: int = 1024 * 1024
DEFAULT_PAYLOAD_FILENAME = "sort_payload"
DEFAULT_TMP_PREFIX = "tmp/"

AWS_S3_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "lab144"
AWS_SECRET_ACCESS_KEY = "astl1a4b4"


def generate_payload(endpoint, partitions, bucket, key, sort_column, sort_output_key=None,
                     delimiter=',', bound_extraction_margin=DEFUALT_BOUND_EXTRACTION_MARGIN,
                     seed=None, tmp_prefix=DEFAULT_TMP_PREFIX):
    if seed is not None:
        random.seed(seed)

    s3_client = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_S3_REGION)

    obj_size = s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    # Avoid dataset head and tail
    start_limit = int(obj_size * DEFAULT_START_MARGIN)
    end_limit = int(obj_size * (1 - DEFAULT_END_MARGIN))
    choosable_size = end_limit - start_limit

    # Size of each sampled fragment
    fragment_size = floor(
        min(
            floor((end_limit - start_limit) * DEFAULT_SAMPLE_RATIO),
            DEFAULT_MAX_SAMPLE_SIZE,
        )
        / DEFAULT_SAMPLE_FRAGMENTS
    )

    # Select bounds randomly
    num_parts = int(choosable_size / fragment_size)
    selected_fragments = sorted(
        random.sample(range(num_parts), DEFAULT_SAMPLE_FRAGMENTS)
    )

    keys_arrays = []
    row_lens = []

    # Read from each bound a fragment size, adjusting limits
    for f in selected_fragments:
        lower_bound = start_limit + f * fragment_size
        upper_bound = lower_bound + fragment_size

        range_0 = max(0, lower_bound - bound_extraction_margin)
        range_1 = min(obj_size, upper_bound + bound_extraction_margin)

        body = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes={range_0}-{range_1}",
        )["Body"].read()

        body_sz = len(body)
        start_byte = lower_bound - range_0
        end_byte = upper_bound - range_1
        if start_byte > 0:
            lower_bound = start_byte

            while lower_bound > 0:
                if body[lower_bound: lower_bound + 1] == b"\n":
                    lower_bound += 1
                    break
                else:
                    lower_bound -= 1
        else:
            lower_bound = 0

        if end_byte < body_sz:
            upper_bound = end_byte

            while upper_bound < body_sz:
                if body[upper_bound: upper_bound + 1] == b"\n":
                    break
                else:
                    upper_bound += 1
        else:
            upper_bound = end_byte

        body_memview = memoryview(body)
        partition = body_memview[lower_bound:upper_bound]

        # find index of \n from the beginning of the body
        buff = BytesIO(partition)
        row_len = len(buff.readline())
        buff.seek(0)
        row_lens.append(row_len)

        df = pd.read_csv(
            BytesIO(partition),
            engine="c",
            index_col=None,
            header=None,
            delimiter=delimiter,
            quoting=3,
            on_bad_lines="warn",
        )

        keys_arrays.append(np.array(df[sort_column]))

    # Assert all row lengths are the same
    assert len(set(row_lens)) == 1
    row_len = set(row_lens).pop()

    # Concat keys, sort them
    keys = np.concatenate(keys_arrays)
    keys.sort()

    # Find quantiles (num tasks)
    quantiles = [i * 1 / partitions for i in range(1, partitions)]
    segment_bounds = [keys[int(q * len(keys))] for q in quantiles]

    # Generate multipart upload
    output_key = (
        sort_output_key
        if sort_output_key is not None
        else key + ".sorted"
    )
    mpu_res = s3_client.create_multipart_upload(Bucket=bucket, Key=output_key)
    print(mpu_res)
    mpu_id = mpu_res["UploadId"]

    pprint(segment_bounds)

    # Write parameters as JSON file
    return [
        {
            "bucket": bucket,
            "key": key,
            "obj_size": obj_size,
            "sort_column": sort_column,
            "delimiter": delimiter,
            "partitions": partitions,
            "partition_idx": i,
            "segment_bounds": segment_bounds,
            "row_size": row_len,
            "mpu_key": output_key,
            "mpu_id": mpu_id,
            "tmp_prefix": tmp_prefix,
            "s3_config": {
                "region": AWS_S3_REGION,
                "endpoint": endpoint,
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            },
        }
        for i in range(partitions)
    ]


def complete_mpu(endpoint, bucket, key, upload_id, mpu):
    s3_client = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_S3_REGION)
    result = s3_client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                                 MultipartUpload=mpu)
    print(result)



