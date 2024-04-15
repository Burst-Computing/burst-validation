import argparse
import json

import pandas as pd

from ow_client.parser import add_openwhisk_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from terasort_utils import generate_payload, add_terasort_to_parser, S3_MAX_PUT_RATE, S3_MAX_GET_RATE

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_terasort_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.ts_endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    # Calculate max request rate
    if args.max_rate_map is None:
        args.max_rate_map = S3_MAX_PUT_RATE // args.partitions - 1
    if args.max_rate_reduce is None:
        args.max_rate_reduce = S3_MAX_GET_RATE // args.partitions - 1

    # Add map and reduce max rate inside s3_config
    params_map = [dict(params[i], s3_config={**params[i].get('s3_config', {}), 'max_rate': args.max_rate_map}) for i in
                  range(args.partitions)]

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    # create pandas dataframe
    stats = pd.DataFrame(columns=["fn_id",
                                  "host_submit_map",
                                  "init_fn_map",
                                  "post_download_map",
                                  "pre_upload_map",
                                  "end_fn_map"])

    host_submit_map = get_millis()
    dt_map = executor.map("terasort-map", params_map,
                          file="terasort/terasort-map.zip",
                          memory=args.runtime_memory if args.runtime_memory else 256,
                          custom_image=args.custom_image, is_zip=True)

    map_results = dt_map.get_results()

    stats = pd.concat([stats,
                       pd.DataFrame({"fn_id": i["partition_idx"],
                                     "host_submit_map": host_submit_map,
                                     "init_fn_map": i["init_fn_map"],
                                     "post_download_map": i["post_download_map"],
                                     "pre_upload_map": i["pre_upload_map"],
                                     "end_fn_map": i["end_fn_map"]
                                     } for i in dt_map.get_results())
                       ])

    # get number of bytes written to each partition for each map worker
    partition_sizes = {i["partition_idx"]: {
        "input_partition_size": i["partition_size"],
        "output_partition_sizes": i["partition_sizes"]
        } for i in map_results}

    stats.to_csv("terasort-classic-onlymap.csv", index=False)
    json.dump(partition_sizes, open("terasort-classic-partition-sizes.json", "w"))

    # complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
    #              mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in
    #                            dt_reduce.get_results().sort(key=lambda x: x['part_number'])]})
    print("**** Terasort-classic (only map) finished ****")
