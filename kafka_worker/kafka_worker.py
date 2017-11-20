# -*- coding: utf-8 -*-

"""Main module."""
from kafka_worker.kafka_consumer import QwantConsumer
from kafka_worker.kafka_producer import QwantProducer
from kafka_worker.config.development import DEFAULT_CONFIG


class QwantWorker(QwantProducer, QwantConsumer):
    def __init__(self):
        super().__init__(**DEFAULT_CONFIG)
        pass
