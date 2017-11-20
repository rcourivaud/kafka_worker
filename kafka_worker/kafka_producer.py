from kafka import KafkaProducer
import abc
from kafka_worker.config.development import DEFAULT_CONFIG
from qwant_logger.qwant_logger import QwantLogger


class QwantProducer(QwantLogger):
    def __init__(self, logger_name, **kwargs):
        super().__init__(logger_name=logger_name, **kwargs)
        self.producer = KafkaProducer(**DEFAULT_CONFIG)

    def send_message(self, message, topic):
        self.producer.send(topic, message)

    def close(self):
        self.producer.close()
