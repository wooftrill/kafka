import os
import logging
from kafka_producer.application.service.KafkaProducer import KafkaProducer,kafka_producer
from kafka_producer.application.service.SQLOrm import sql_orm_service

logging.getLogger().setLevel(logging.INFO)


class KafkaProducerService:
    def __init__(self):
        pass

    def produce_to_topic(self,req_body):
        try:
            if req_body:
                sql_response= sql_orm_service.insert_to_order_submit(req_body)
                if sql_response is None:
                    logging.error("Issue with Inset_to_order function.. ")
                    return False
                else:
                    logging.info("Pushing to kafka")
                    print(sql_response)
                    return kafka_producer.producer(sql_response)
        except Exception as ex :
            logging.exception(ex)


kafka_producer_service= KafkaProducerService()
