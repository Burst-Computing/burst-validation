import base64
import sys
import time
import warnings

import requests
from typing import List
from urllib3.exceptions import InsecureRequestWarning

from ow_client import utils
from ow_client.result_dataset import ResultDataset
from ow_client.utils import debug, ppdegub

AUTH_TOKEN = "MjNiYzQ2YjEtNzFmNi00ZWQ1LThjNTQtODE2YWE0ZjhjNTAyOjEyM3pPM3haQ0xyTU42djJCS0sxZFhZRnBYbFBrY2NPRnFtMTJDZEFzTWdSVTRWck5aOWx5R1ZDR3VNREdJd1A="


class OpenwhiskExecutor:
    def __init__(self, host, port, debug=False):
        self.protocol = 'http'
        self.host = host
        self.port = port
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Basic {AUTH_TOKEN}", "Content-Type": "application/json"})
        self.session.verify = False
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        self.__monitor_interval = 2  # seconds
        utils.DEBUGGING = True

    def burst(self, action_name, params_list, is_zip=False, memory=256, custom_image=None, backend="rabbitmq",
              burst_size=None, chunk_size=1, join=False) -> ResultDataset:
        """
        Function to invoke a burst of actions
        :param action_name: the name of the action to invoke. Action must be located into functions folder.
        Action name not include the extension of the file, but file must be named as functions/<action_name>{.rs, .zip}
        :param params_list: list with the parameters to pass to the actions (list of dicts)
        :param is_zip: indicates if the action is a zip file or a single .rs file
        :param memory: memory to allocate to the action
        :param custom_image: if not None, the action is executed in a container with the specified image
        :param backend: the backend to use for the burst. (Rabbitmq, RedisStream...)
        :param burst_size: granularity of the burst. If None, the burst is executed in heterogeneous mode
        :param chunk_size: in burst comm middleware message exchanges (in KB)
        :param join: if True, the burst is executed in heterogeneous containers that respects the multiplicity of the burst size
        :return: Dataset with the results and some metrics of the executions
        """
        dataset = ResultDataset()
        self.__create_action(action_name, is_zip, memory, custom_image)
        activation_ids = self.__invoke_burst_actions(action_name, params_list, burst_size, backend, chunk_size, join)
        for index, activation_id in enumerate(activation_ids):
            dataset.add_invocation(index, activation_id, time.time(), is_burst=True)
        fetch_count = 0
        self.__wait_for_completion(dataset)
        return dataset

    def map(self, action_name, params_list, is_zip=False, memory=256, custom_image=None) -> ResultDataset:
        """
        Function to invoke a map (classic) of actions
        :param action_name: the name of the action to invoke. Action must be located into functions folder.
        Action name not include the extension of the file, but file must be named as functions/<action_name>{.rs, .zip}
        :param params_list: list with the parameters to pass to the actions (list of dicts)
        :param is_zip: indicates if the action is a zip file or a single .rs file
        :param memory: memory to allocate to the action
        :param custom_image: if not None, the action is executed in a container with the specified image
        :return: Dataset with the results and some metrics of the executions
        """
        dataset = ResultDataset()
        self.__create_action(action_name, is_zip, memory, custom_image)
        for index, input in enumerate(params_list):
            activation_id = self.__invoke_single_action(action_name, input)
            dataset.add_invocation(index, activation_id, time.time(), is_burst=False)
        self.__wait_for_completion(dataset)
        return dataset

    def __create_action(self, action_name, is_zip, memory, custom_image):
        action_data = {
            "exec": {
                "main": "main",
                "kind": "rust:1.34"
            },
            "name": action_name,
            "version": "0.0.1",
            "namespace": "guest",
            "limits": {
                "memory": memory,
            }
        }
        if is_zip:
            code = open(f"ow_functions/{action_name}.zip", "rb").read()
            code = (base64.b64encode(code)).decode('ascii')
            binary = True
        else:
            action_file = f"ow_functions/{action_name}.rs"
            with open(action_file, "r") as rust_file:
                rust_code = rust_file.read()
            code = rust_code.strip()
            binary = False

        action_data["exec"]["binary"] = binary
        action_data["exec"]["code"] = code

        if custom_image:
            action_data["exec"]["image"] = custom_image
            action_data["exec"]["kind"] = "blackbox"

        response = self.session.put(
            f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}?overwrite=true",
            json=action_data)

        if response.status_code == 200:
            debug(f"Función {action_name} creada con éxito en OpenWhisk")
        else:
            debug(f"Error al crear la función {action_name}: {response.text}")

    def __invoke_single_action(self, action_name, params):
        invoke_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}"

        response = self.session.post(invoke_url, json=params)

        if str(response.status_code).startswith("2"):
            debug(f"Función {action_name} invocada con éxito en OpenWhisk")
            result = response.json()
            return result["activationId"]
        else:
            debug(f"Error al invocar la función {action_name}: {response.text}")
            return None

    def __check_function_finished(self, activation_id):
        activation_get_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/activations/{activation_id}"
        response = self.session.get(activation_get_url)
        if str(response.status_code).startswith("2"):
            debug(f"Función {activation_id} consultada con éxito en OpenWhisk")
            ppdegub(response.json())
            result_invk = response.json()
            if result_invk["response"]["result"]:
                debug(f"Función {activation_id} finalizada con éxito en OpenWhisk")
                return result_invk
            else:
                debug(f"Función {activation_id} finalizada con error en OpenWhisk")
                return None
        else:
            debug(f"Error al consultar la función {activation_id}: {response.text}")
            return None

    def __wait_for_completion(self, dataset):
        monitor_count = 0
        while any("result" not in item for item in dataset.results):
            missing_results = [item for item in dataset.results if "result" not in item]
            for item in missing_results:
                result = self.__check_function_finished(item["activationId"])
                if result:
                    dataset.add_result(item["activationId"], result["start"], result["end"],
                                       result["response"]["result"])
            monitor_count += 1
            debug(f"Monitor count: {monitor_count}")
            time.sleep(self.__monitor_interval)
        return dataset

    def __invoke_burst_actions(self, action_name, params_list, burst_size,
                               backend, chunk_size, join) -> List[str] | None:
        burst_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}?burst=true"
        if burst_size:
            burst_url += f"&granularity={burst_size}"
        if join:
            burst_url += "&join=true"
        burst_url += f"&backend={backend}&chunk_size={chunk_size}"
        params_list = {"value": params_list}
        response = self.session.post(burst_url, json=params_list)

        if str(response.status_code).startswith("2"):
            debug(f"Burst {action_name} invocado con éxito en OpenWhisk")
            result = response.json()
            return result["activationIds"]
        else:
            debug(f"Error al invocar el burst {action_name}: {response.text}")
            return None
