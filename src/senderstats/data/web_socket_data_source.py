import json
import os
import queue
import ssl
import threading
from datetime import datetime, timedelta

import websocket

from senderstats.core.mappers.json_mapper import JSONMapper
from senderstats.interfaces.data_source import DataSource

print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    """A thread-safe print function."""
    with print_lock:
        print(*args, **kwargs)


class WebSocketDataSource(DataSource):
    MODULES = ["dkimv", "spf", "dmarc"]

    def __init__(self, field_mapper: JSONMapper, cluster_id: str, token: str, log_type: str, timeout: int = 300):
        self.__cluster_id = cluster_id
        self.__token = token
        self.__log_type = log_type
        self.__header = {"Authorization": f"Bearer {token}"}
        self.__ssl_options = {"cert_reqs": ssl.CERT_NONE}
        self.__timeout = timeout
        self.__field_mapper = field_mapper
        self.__input_queue = queue.Queue()
        self.__output_queue = queue.Queue()
        self.__production_done = threading.Event()
        current_time = datetime.now().astimezone()
        start_time = (current_time - timedelta(days=30))
        end_time = current_time
        self.__generate_time_windows(start_time,end_time)
        self.start_workers(os.cpu_count())
        self.__print_lock = threading.Lock()

    def __build_websocket_url(self, start_time, end_time):
        """Helper function to construct the WebSocket URL."""
        base_url = f"wss://logstream.proofpoint.com:443/v1/stream?cid={self.__cluster_id}&type={self.__log_type}"
        return f"{base_url}&sinceTime={start_time}&toTime={end_time}"

    def __generate_time_windows(self, start_time, end_time):
        # Initialize the current time to the start time
        current_time = start_time

        # Generate one-hour windows until reaching the end time
        while current_time < end_time:
            # Define the end of the current window
            window_end = min(current_time + timedelta(hours=1), end_time)

            # Append the time window as a tuple
            self.__input_queue.put((current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + current_time.strftime('%z'), window_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + window_end.strftime('%z')))

            # Move to the next hour
            current_time = window_end + timedelta(milliseconds=1)

    def start_workers(self, num_workers):
        self.__workers = []
        for i in range(num_workers):
            worker_thread = threading.Thread(target=self.__input_worker, name=f"Worker-{i}")
            worker_thread.start()
            self.__workers.append(worker_thread)


    def read_data(self):
        while not (self.__production_done.isSet() and self.__output_queue.empty()):
            try:
                yield self.__output_queue.get(timeout=0.1)
                self.__output_queue.task_done()
            except queue.Empty:
                continue

    def __input_worker(self):
        while True:
            task = self.__input_queue.get()
            if task is None:  # Check for termination signal
                self.__input_queue.task_done()
                break

            websocket_url = self.__build_websocket_url(task[0], task[1])
            ws = websocket.create_connection(websocket_url, header=self.__header,
                                             sslopt=self.__ssl_options, timeout=self.__timeout)
            data_count = 0
            try:
                while ws.connected:
                    result = ws.recv()
                    if result:
                        result = self.__process_result(result) if self.__log_type == "message" else result.strip("\n")
                        data = self.__field_mapper.map_fields(result)
                        if data.direction == 'outbound':
                            self.__output_queue.put(data)
                        data_count += 1
            except Exception as e:
                thread_safe_print(f"ERROR: Failed to fetch data: {e}. Total records fetched: {data_count} [{threading.current_thread().name}]")
            finally:
                ws.close()
                thread_safe_print(f"{task[0]} --> {task[1]} - Successfully collected {data_count} records. [{threading.current_thread().name}]")

            # Mark the task as done in the input queue
            self.__input_queue.task_done()

        # Exit the loop, we are done with processing
        self.__production_done.set()

    @staticmethod
    def __process_result(result):
        """Process JSON result based on action modules."""
        result = json.loads(result)
        field_filter = result.get("filter", {})
        actions = field_filter.get("actions", [])

        grouped_actions = {module: [] for module in WebSocketDataSource.MODULES}
        for action in actions:
            module = action.get("module")
            if module in grouped_actions:
                grouped_actions[module].append(action)
            if action.get("isFinal"):
                result.update({
                    "final_action": action.get("action"),
                    "final_module": module,
                    "final_rule": action.get("rule"),
                })

        result.update({f"action_{mod}": actions for mod, actions in grouped_actions.items()})
        return result
