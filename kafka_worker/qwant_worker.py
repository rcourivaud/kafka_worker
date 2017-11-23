import abc
from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer


class QwantWorker(QwantProducer, QwantConsumer):
    def __init__(self, bootstrap_servers, listen_topics, answer_topic, group_id):
        super().__init__(logger_name=self.__class__,
                         bootstrap_servers=bootstrap_servers,
                         topics=listen_topics,
                         topic=answer_topic,
                         group_id=group_id)

        self.extra = {"bootstrap_servers": bootstrap_servers,
                      "group_id": listen_topics,
                      "answer_topic": answer_topic}

    def launch(self, commit=True):
        for message in self.consumer:
            # self.debug("Getting message",extra={"kafka_key":message.key})
            print(message)
            print(type(message))
            data = self.process(value=message.value, key=message.key)
            self.send_message(message=data,
                              key=message.key)
            if commit:
                self.consumer.commit(message.offset)

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()
