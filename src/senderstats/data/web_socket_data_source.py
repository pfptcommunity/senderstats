from senderstats.interfaces.data_source import DataSource


class WebSocketDataSource(DataSource):
    def __init__(self, websocket_url):
        self.websocket_url = websocket_url

    def read_data(self):
        # Implement WebSocket data reading logic
        pass
