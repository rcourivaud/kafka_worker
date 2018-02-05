# -*- coding: utf-8 -*-

"""Main module."""
import abc
import json

from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from qwant_logger.qwant_logger import QwantLogger

from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer


class QwantWorker(QwantLogger):
    def __init__(self, bootstrap_servers, listen_topics, answer_topic, group_id=None):
        super().__init__(logger_name=self.__class__)

        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.listen_topics = listen_topics
        self.answer_topic = answer_topic

        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                      group_id=self.group_id,
                                      enable_auto_commit=False)

        self.consumer.subscribe(listen_topics)

        self.extra = {"listen_topics": listen_topics,
                      "answer_topic": answer_topic,
                      "bootstrap_servers": bootstrap_servers}

    def send_message(self, message, topic=None, key=None, partition=None):
        if topic is None:
            topic = self.answer_topic
        self.producer.send(topic=topic,
                           value=message.encode(),
                           key=key,
                           partition=partition)
        self.info("send message", extra={"message": "message"})

    def launch(self, commit=True):
        for message in self.consumer:
            self.debug("Getting message", )
            data = self.process(value=json.load(message.value),
                                key=message.key)

            self.send_message(message=data,key=message.key)
            if commit:
                self.consumer.commit()

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()


if __name__ == "__main__":
    qw = Worker(bootstrap_servers="172.16.100.10:6667",
                     listen_topics=["bubing-test"],
                     answer_topic="bubing-test-response", group_id=1)
    qw.process = lambda key, value: value
    qw.launch(commit=False)
