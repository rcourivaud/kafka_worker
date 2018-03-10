import json

from kafka import KafkaConsumer

class KafkaConsumer(QwantLogger):
    def __init__(self, logger_name, topics, group_id, bootstrap_servers, fetch_max_bytes):
        print("kafka_consumer : %s" % bootstrap_servers)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      enable_auto_commit=False,
                                      value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                                      auto_offset_reset="earliest",
                                      fetch_max_bytes=fetch_max_bytes)

        self.listen_topics = topics
        self.consumer.subscribe(self.listen_topics)

