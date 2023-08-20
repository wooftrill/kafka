import os
import logging
import time

from kafka_producer.utils.HelperUtils import HelperUtils
from kafka_producer.config.Config import KAFKA
from kafka_producer.utils.KafkaError import kafka_error

logging.getLogger().setLevel(logging.INFO)


class KafkaProducer:

    def __init__(self):
        pass

    def producer(self,message: dict):
        retry = 0
        _max_retries=3
        while retry < _max_retries:
            try:
                if message:
                    producer = HelperUtils.kafka_producer_client(KAFKA["address"])
                    response = producer.send(KAFKA["topic"], value=message.encode('utf-8'))
                    metadata = response.get(timeout=10)
                    logging.info(f'Message delivered to topic:{metadata.topic} in Partition: {metadata.partition} with Offset: {metadata.offset}')
                    producer.close()
                    return True
            except(kafka_error.NoBrokersAvailable, kafka_error.KafkaError,kafka_error.KafkaTimeoutError, Exception) as e:
                logging.error(f'Exception occurred: {str(e)}')
                time.sleep(1)
                logging.info(f'Retrying in 1 seconds...')
                retry += 1
        return False


kafka_producer = KafkaProducer()









