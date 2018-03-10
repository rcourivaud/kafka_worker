import json

from kafka import KafkaProducer


class WorkerProducer:
    def __init__(self, topic, bootstrap_servers, batch_size, linger_ms):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                      batch_size=batch_size,
                                      linger_ms=linger_ms)
        self.topic = topic

    def send_message(self, message, topic=None, key=None, partition=None):
        if topic is None:
            topic = self.topic
        self.producer.send(topic=topic, value=message, key=key, partition=partition)

    def close(self):
        self.producer.close()
