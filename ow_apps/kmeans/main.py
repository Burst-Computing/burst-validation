import argparse
import json
import pprint

from ow_client.openwhisk_executor import OpenwhiskExecutor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--granularity", type=int, required=False, help="Granularity of burst workers", default=None)
    parser.add_argument("--join", type=bool, required=False, help="Join burst workers in same invoker", default=False)
    parser.add_argument("--backend", type=str, required=True, help="Burst communication backend")
    parser.add_argument("--chunk_size", type=int, required=False, help="Chunk size for burst messages", default=1)

    args = parser.parse_args()

    executor = OpenwhiskExecutor("172.17.0.1", 3233)
    params = json.load(open("ow_apps/kmeans/payload2.json"))
    dt = executor.burst("kmeans-burst",
                        params,
                        memory=4096,
                        burst_size=args.granularity,
                        join=args.join,
                        backend=args.backend,
                        chunk_size=args.chunk_size,
                        custom_image="manriurv/rust-burst:1.72.1",
                        is_zip=True)
    dt.plot()
    pprint.pprint(dt.get_results())
