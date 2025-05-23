import argparse

import pandas as pd

from ow_client.parser import add_openwhisk_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from terasort_utils import generate_payload, complete_mpu, add_terasort_to_parser, S3_MAX_PUT_RATE, S3_MAX_GET_RATE

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
    params_reduce = [dict(params[i], s3_config={**params[i].get('s3_config', {}), 'max_rate': args.max_rate_reduce}) for i in range(args.partitions)]


    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    # create pandas dataframe
    stats = pd.DataFrame(columns=["fn_id",
                                  "host_submit_reduce",
                                  "init_fn_reduce",
                                  "post_download_reduce",
                                  "pre_upload_reduce",
                                  "end_fn_reduce",
                                  "finished"])

    host_submit_reduce = get_millis()
    dt_reduce = executor.map("terasort-reduce", params_reduce,
                             file="terasort/terasort-reduce.zip",
                             memory=args.runtime_memory if args.runtime_memory else 256,
                             custom_image=args.custom_image, is_zip=True)
    end_time = get_millis()
    stats = pd.concat([stats,
                       pd.DataFrame({"fn_id": i["part_number"],
                                     "host_submit_reduce": host_submit_reduce,
                                     "init_fn_reduce": i["init_fn_reduce"],
                                     "post_download_reduce": i["post_download_reduce"],
                                     "pre_upload_reduce": i["pre_upload_reduce"],
                                     "end_fn_reduce": i["end_fn_reduce"],
                                     "finished": end_time
                                     } for i in dt_reduce.get_results() if "part_number" in i)
                       ])

    stats.to_csv("terasort-classic-onlyreduce.csv", index=False)

    # complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
    #              mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in
    #                            dt_reduce.get_results().sort(key=lambda x: x['part_number'])]})
    print("**** Terasort-classic (only-reduce) finished ****")
