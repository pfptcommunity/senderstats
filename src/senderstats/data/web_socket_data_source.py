import json
import ssl
from datetime import datetime, timedelta

import websocket

from senderstats.core.mappers.json_mapper import JSONMapper
from senderstats.interfaces.data_source import DataSource


class WebSocketDataSource(DataSource):
    MODULES = ["dkimv", "spf", "dmarc"]

    def __init__(self, field_mapper: JSONMapper, cluster_id: str, token: str, log_type: str, timeout: int = 300):
        self.__websocket_url = self._build_websocket_url(cluster_id, log_type)
        self.__log_type = log_type
        self.__header = {"Authorization": f"Bearer {token}"}
        self.__ssl_options = {"cert_reqs": ssl.CERT_NONE}
        self.__timeout = timeout
        self.__field_mapper = field_mapper

    def _build_websocket_url(self, cluster_id, log_type):
        base_url = f"wss://logstream.proofpoint.com:443/v1/stream?cid={cluster_id}&type={log_type}"
        current_time = datetime.now()
        since_time = (current_time - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S%z")
        to_time = current_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        return f"{base_url}&sinceTime={since_time}&toTime={to_time}"

    def read_data(self):
        ws = websocket.create_connection(self.__websocket_url, header=self.__header,
                                         sslopt=self.__ssl_options, timeout=self.__timeout)
        data_count = 0
        try:
            while ws.connected:
                result = ws.recv()
                if result:
                    result = self.__process_result(result) if self.__log_type == "message" else result.strip("\n")
                    data = self.__field_mapper.map_fields(result)
                    if data.direction == 'outbound':
                        yield data
                    data_count += 1
                    if data_count % 5000 == 0:
                        print(f"Records fetched so far: {data_count}")

        except Exception as e:
            print(f"ERROR: Failed to fetch data: {e}. Total records fetched: {data_count}")
        finally:
            ws.close()
            print(f"Successfully collected {data_count} records.")

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
