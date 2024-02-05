import json
import pprint

from ow_client.openwhisk_executor import OpenwhiskExecutor

if __name__ == "__main__":
    executor = OpenwhiskExecutor("172.17.0.1", 3233)
    params = json.load(open("use_cases/kmeans/payload2.json"))
    dt = executor.burst("kmeans-burst", params, memory=4096,
                        custom_image="manriurv/rust-burst:1.72.1", is_zip=True)
    dt.plot()
    pprint.pprint(dt.get_results())
