from kafka import KafkaProducer
from qwant_logger.qwant_logger import QwantLogger
import json


class QwantProducer(QwantLogger):
    def __init__(self, logger_name, topic, group_id, bootstrap_servers):
        print("kafka_producer : %s" % bootstrap_servers)
        super().__init__(logger_name=logger_name, topics=topic, group_id=group_id, bootstrap_servers=bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic

    def send_message(self, message, topic=None, key=None, partition=None):
        if topic is None:
            topic = self.topic
        self.producer.send(topic=topic, value=message, key=key, partition=partition)
        #self.info("send message", extra={"kafka_message": "message"})

    def close(self):
        self.producer.close()
