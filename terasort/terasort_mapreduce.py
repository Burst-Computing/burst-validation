import argparse

import pandas as pd

from ow_client.parser import add_openwhisk_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from terasort_utils import generate_payload, complete_mpu, add_terasort_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_terasort_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.ts_endpoint, partitions=args.partitions, bucket=args.bucket, key=args.key,
                              sort_column=0)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    # create pandas dataframe
    stats = pd.DataFrame(columns=["fn_id",
                                  "host_submit_map",
                                  "init_fn_map",
                                  "post_download_map",
                                  "pre_upload_map",
                                  "end_fn_map",
                                  "host_submit_reduce",
                                  "init_fn_reduce",
                                  "post_download_reduce",
                                  "pre_upload_reduce",
                                  "end_fn_reduce",
                                  "finished"])

    host_submit_map = get_millis()
    dt_map = executor.map("terasort-map", params,
                          memory=args.runtime_memory if args.runtime_memory else 256,
                          custom_image=args.custom_image, is_zip=True)

    stats = pd.concat([stats,
                       pd.DataFrame({"fn_id": i["partition_idx"],
                                     "host_submit_map": host_submit_map,
                                     "init_fn_map": i["init_fn_map"],
                                     "post_download_map": i["post_download_map"],
                                     "pre_upload_map": i["pre_upload_map"],
                                     "end_fn_map": i["end_fn_map"]
                                     } for i in dt_map.get_results())
                       ])

    host_submit_reduce = get_millis()
    dt_reduce = executor.map("terasort-reduce", params,
                             memory=args.runtime_memory if args.runtime_memory else 256,
                             custom_image=args.custom_image, is_zip=True)

    end_time = get_millis()
    for i in dt_reduce.get_results():
        stats.loc[stats["fn_id"] == i["part_number"], "host_submit_reduce"] = host_submit_reduce
        stats.loc[stats["fn_id"] == i["part_number"], "init_fn_reduce"] = i["init_fn_reduce"]
        stats.loc[stats["fn_id"] == i["part_number"], "post_download_reduce"] = i["post_download_reduce"]
        stats.loc[stats["fn_id"] == i["part_number"], "pre_upload_reduce"] = i["pre_upload_reduce"]
        stats.loc[stats["fn_id"] == i["part_number"], "end_fn_reduce"] = i["end_fn_reduce"]
        stats.loc[stats["fn_id"] == i["part_number"], "finished"] = end_time

    stats.to_csv("terasort-classic-stats.csv", index=False)

    # complete_mpu(endpoint=args.endpoint, bucket=args.bucket, key=params[0]['mpu_key'], upload_id=params[0]["mpu_id"],
    #              mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in
    #                            dt_reduce.get_results().sort(key=lambda x: x['part_number'])]})
    print("**** Terasort-classic finished ****")
