import json

from kafka import KafkaConsumer, TopicPartition
from qwant_logger.qwant_logger import QwantLogger


class QwantConsumer(QwantLogger):
    def __init__(self, logger_name, topics, group_id, bootstrap_servers):
        with open('out.txt', 'a') as f:
            print("kafka_consumer : %s" % bootstrap_servers,  file=f)

        super().__init__(logger_name=logger_name, topics=topics, group_id=group_id, bootstrap_servers=bootstrap_servers)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      enable_auto_commit=False,
                                      value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        self.listen_topics = topics
        #self.consumer.subscribe(self.listen_topics)
        self.topic_partion = TopicPartition(topic=topics[0], partition=0)
        self.consumer.assign([self.topic_partion])
