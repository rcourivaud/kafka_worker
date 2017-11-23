import abc
import json

from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer


class QwantWorker(QwantProducer, QwantConsumer):
    def __init__(self, bootstrap_servers, listen_topics, answer_topic, group_id):
        print("qwant_worker : %s" % bootstrap_servers)
        QwantProducer.__init__(self, self.__class__, answer_topic, group_id, bootstrap_servers)
        QwantConsumer.__init__(self, self.__class__, listen_topics, group_id, bootstrap_servers)

        self.extra = {"bootstrap_servers": bootstrap_servers,
                      "group_id": listen_topics,
                      "answer_topic": answer_topic}

    def launch(self, commit=True):
        for message in self.consumer:
            # self.debug("Getting message",extra={"kafka_key":message.key})
            data = self.process(value=message.value, key=message.key)
            self.send_message(message=data,
                              key=message.key)
            if commit:
                self.consumer.commit(message.offset)

            break

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()


if __name__ == "__main__":
    qw = QwantWorker(bootstrap_servers="localhost:9092",
                     listen_topics=["topic_test"],
                     answer_topic="response", group_id=1)
    qw.process = lambda value, key: {"ok": 1}
    qw.launch(commit=False)
