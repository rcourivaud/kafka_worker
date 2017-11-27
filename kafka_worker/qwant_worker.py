import abc
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
            data = self.process(value=message.value, key=message.key)
            self.send_message(message=data,
                              key=message.key)
            if commit:
                self.consumer.commit(message.offset)

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()
