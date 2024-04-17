import argparse
import pandas as pd
import json

from ow_client.parser import add_openwhisk_to_parser, add_burst_to_parser, try_or_except
from ow_client.time_helper import get_millis
from ow_client.openwhisk_executor import OpenwhiskExecutor
from pagerank_utils import generate_payload, add_pagerank_to_parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_pagerank_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    params = generate_payload(endpoint=args.pr_endpoint, partitions=args.partitions, num_nodes=args.num_nodes, bucket=args.bucket, 
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

    results = [{"fn_id": i["key"],
                "host_submit": host_submit,
                "timestamps": i["timestamps"],
                "finished": finished
                } for i in flattened_results]

    json.dump(results, open("pagerank-burst.json", "w"))
