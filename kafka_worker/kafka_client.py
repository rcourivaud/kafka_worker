from kafka import KafkaClient

from kafka_worker.config.development import DEFAULT_CONFIG


class QwantProducer(QwantLogger):
    def __init__(self):
        self.consumer = Su(**DEFAULT_CONFIG)
        pass

    @abc.abstractmethod
    def process(self, message):
        raise NotImplementedError()

    def launch(self):
        for message in self.consumer:
            self.process(message=message)

