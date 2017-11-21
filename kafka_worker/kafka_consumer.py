import json

from kafka import KafkaConsumer
import abc
from qwant_logger.qwant_logger import QwantLogger


class QwantConsumer(QwantLogger):
    def __init__(self,
                 logger_name,
                 topics, group_id=None,
                 **kwargs):
        super().__init__(logger_name=logger_name, **kwargs)
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                      group_id=group_id,
                                      enable_auto_commit=False)
        self.consumer.subscribe(topics)
        self.extra = {"topics": topics, "group_id": group_id}




if __name__ == "__main__":
    qc = QwantConsumer("consumer test", topics=["topic_test"])
    qc.process = lambda message: print(message)
    qc.launch()
