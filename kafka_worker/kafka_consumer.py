import abc
import json

from kafka import KafkaConsumer
from multiprocessing_generator import ParallelGenerator


class WorkerConsumer:
    def __init__(self, topics, group_id, bootstrap_servers, fetch_max_bytes):
        print("kafka_consumer : %s" % bootstrap_servers)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      enable_auto_commit=False,
                                      value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                                      auto_offset_reset="earliest",
                                      fetch_max_bytes=fetch_max_bytes)

        self.listen_topics = topics
        self.consumer.subscribe(self.listen_topics)

    def launch(self, commit=True, multiprocess=False):
        if multiprocess:
            with ParallelGenerator(self.consumer, max_lookahead=100) as g:
                for message in g:
                    _ = self.process(value=message.value, key=message.key)
                    if commit:
                        self.consumer.commit()
        else:
            for message in self.consumer:
                _ = self.process(value=message.value, key=message.key)
                if commit:
                    self.consumer.commit()

    @abc.abstractmethod
    def process(self, value, key):
        raise NotImplementedError()


if __name__ == "__main__":
    a = WorkerConsumer(["rec.scrapy.extracted"], "1", "51.255.48.93:9092", 200000)
    a.process = lambda value, key: print(value)
    a.launch(commit=False)
