
class StorageSettings:
    def __init__(self, config):
        self.host = config.get("broker_host", "localhost:9092")
        self.topic = config.get("topic", "iot_gateway")