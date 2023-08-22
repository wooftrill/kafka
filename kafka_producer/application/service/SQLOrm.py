import json
import os
import logging
from kafka_producer.application.service.SQLClient import SQLClient,sql_client

from kafka_producer.config.Config import TABLE



class SQLOrmService(SQLClient):
    def __init__(self):
        super().__init__()

        self.__checkout_table=TABLE["checkout"]
        self.__biller_table = TABLE["biller"]

    def insert_to_order_submit(self,request_body: dict):
        logging.info("Inserting to Order submit")
        try:
            if request_body:
                order_details = sql_client.get_record(self.__checkout_table,request_body["session_id"],request_body["uid"])
                print("yhh",order_details)
                if order_details:
                    logging.info("corresponding record found")
                    request_body["net_total"]=json.loads(order_details[0]["checkout_details"])["net_total"]
                    request_body["full_order"] = json.loads(order_details[0]["full_order"])
                    if request_body:
                        if sql_client.insert(self.__biller_table,request_body):
                            return request_body

        except Exception as e :
            logging.error(e)


sql_orm_service= SQLOrmService()

