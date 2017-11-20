from kafka import KafkaConsumer
import abc
from qwant_logger.qwant_logger import QwantLogger

class QwantConsumer(QwantLogger):
    def __init__(self, logger_name, **kwargs):
        super().__init__(logger_name=logger_name, **kwargs)
        self.consumer = KafkaConsumer(**DEFAULT_CONFIG)

    def launch(self):
        for message in self.consumer:
            self.process(message=message)

    @abc.abstractmethod
    def process(self, message):
        raise NotImplementedError()
