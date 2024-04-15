import argparse
import pandas as pd

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from pagerank_utils import generate_payload, complete_mpu, add_pagerank_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_pagerank_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.ts_endpoint, partitions=args.partitions, num_nodes=args.num_nodes, bucket=args.bucket, 
                              key=args.key)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port, args.debug)

    host_submit = get_millis()
    dt = executor.burst("pagerank",
                        params,
                        file="pagerank/pagerank.zip",
                        memory=args.runtime_memory if args.runtime_memory else 4096,
                        custom_image=args.custom_image,
                        debug_mode=args.debug,
                        burst_size=args.granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        is_zip=True)
    finished = get_millis()

    flattened_results = [item for sublist in dt.get_results() for item in sublist]
    flattened_results.sort(key=lambda x: x['key'])

    stats = pd.DataFrame({"fn_id": i["key"],
                          "host_submit": host_submit,
                          "timestamps": i["timestamps"],
                          "finished": finished
                          } for i in flattened_results)

    stats.to_csv("pagerank.csv", index=False)

    # complete_mpu(endpoint=args.ts_endpoint, bucket=args.bucket, key=params[0]['mpu_key'],
    #              upload_id=params[0]["mpu_id"],
    #              mpu={"Parts": [{"ETag": i['etag'], "PartNumber": i['part_number'] + 1} for i in flattened_results]})
    print("Dummy MPU completion done!")
