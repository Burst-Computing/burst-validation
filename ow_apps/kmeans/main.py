import argparse
import json
import pprint

from ow_apps.helpers.parser import add_burst_to_parser, try_or_except, add_openwhisk_to_parser
from ow_client.openwhisk_executor import OpenwhiskExecutor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port)
    # TODO: parametrise data that now is hardcoded in json
    params = json.load(open("ow_apps/kmeans/payload2.json"))
    params = [params[0] for _ in range(4)]
    dt = executor.burst("kmeans-burst",
                        params,
                        memory=1024,
                        burst_size=args.granularity,
                        join=args.join,
                        debug_mode=args.debug,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        custom_image="manriurv/rust_burst:1",
                        is_zip=True)
    dt.plot()
    pprint.pprint(dt.get_results())
