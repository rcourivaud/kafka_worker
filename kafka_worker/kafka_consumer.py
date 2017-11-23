from kafka import KafkaConsumer
from qwant_logger.qwant_logger import QwantLogger


class QwantConsumer(QwantLogger):
    def __init__(self,
                 logger_name,
                 topics, group_id,
                 bootstrap_servers='localhost:9092',
                 **kwargs):
        super().__init__(logger_name=logger_name, **kwargs)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      group_id=group_id,
                                      enable_auto_commit=False)
        self.consumer.subscribe(topics)


if __name__ == "__main__":
    qc = QwantConsumer(bootstrap_servers="localhost:9092",
                       logger_name="consumer test", topics=["topic_test"])
    qc.process = lambda message: print(message)
    qc.launch()
