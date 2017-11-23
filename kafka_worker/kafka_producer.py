from kafka import KafkaProducer
from qwant_logger.qwant_logger import QwantLogger
import json


class QwantProducer(QwantLogger):
    def __init__(self, logger_name, topic, bootstrap_servers, **kwargs):
        super().__init__(logger_name=logger_name, **kwargs)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic

    def send_message(self, message, topic=None, key=None, partition=None):
        if topic is None:
            topic = self.topic
        self.producer.send(topic=topic, value=message, key=key, partition=partition)
        print("sended")
        #self.info("send message", extra={"kafka_message": "message"})

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    qp = QwantProducer(logger_name="test", topic='produce', bootstrap_servers="localhost:9092")

    with open("./../data/hit{}.json".format(2), 'r', encoding='utf-8') as f:
        data = json.load(f)
    qp.send_message(message=data)
    qp.close()
