import abc

from multiprocessing_generator import ParallelGenerator

from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer


class Worker(QwantProducer, QwantConsumer):
    def __init__(self, bootstrap_servers, listen_topics, answer_topic, group_id, batch_size, linger_ms,
                 fetch_max_bytes):
        print("worker : %s" % bootstrap_servers)

        QwantProducer.__init__(self, self.__class__,
                               topic=answer_topic,
                               bootstrap_servers=bootstrap_servers,
                               batch_size=batch_size,
                               linger_ms=linger_ms)
        QwantConsumer.__init__(self, self.__class__,
                               topics=listen_topics,
                               group_id=group_id,
                               bootstrap_servers=bootstrap_servers,
                               fetch_max_bytes=fetch_max_bytes)

        self.extra = {"bootstrap_servers": bootstrap_servers,
                      "group_id": listen_topics,
                      "answer_topic": answer_topic}

    def launch(self, commit=True):
        with ParallelGenerator(self.consumer, max_lookahead=100) as g:
            for message in g:
                data = self.process(value=message.value, key=message.key)
                self.send_message(message=data,
                                  key=message.key)
                if commit:
                    self.consumer.commit()

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()
