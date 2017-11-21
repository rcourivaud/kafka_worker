# -*- coding: utf-8 -*-

"""Main module."""
import abc
import json

from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer


class QwantWorker(QwantProducer, QwantConsumer):
    def __init__(self, bootstrap_servers, listen_topics, answer_topic):
        super().__init__(logger_name=self.__class__,
                         bootstrap_servers=bootstrap_servers,
                         topics=listen_topics,
                         topic=answer_topic)

    def launch(self):
        for message in self.consumer:
            self.debug("Getting message", )
            data = self.process(value=json.load(message.value), key=message.key)
            self.send_message(message=data,
                              key=message.key)
            self.consumer.commit()

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()


if __name__ == "__main__":
    qw = QwantWorker(bootstrap_servers="localhost:9092",
                     listen_topics=["listen"],
                     answer_topic="response")
    qw.launch()
