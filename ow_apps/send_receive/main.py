import argparse
import pprint

from ow_apps.helpers.parser import add_burst_to_parser, try_or_except, add_openwhisk_to_parser
from ow_client.openwhisk_executor import OpenwhiskExecutor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_openwhisk_to_parser(parser)
    add_burst_to_parser(parser)
    args = try_or_except(parser)

    executor = OpenwhiskExecutor(args.ow_host, args.ow_port)
    dt = executor.burst("send-receive",
                        [{"key": "value"}, {"key": "value"}],
                        memory=args.runtime_memory if args.runtime_memory else 4096,
                        burst_size=args.granularity,
                        join=args.join,
                        debug_mode=args.debug,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        custom_image=args.custom_image,
                        is_zip=True)
    dt.plot()
    pprint.pprint(dt.get_results())
