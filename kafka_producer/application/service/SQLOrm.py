
import json
import os
import logging
from kafka_producer.utils.HelperUtils import HelperUtils
from kafka_producer.application.service.SQLClient import SQLClient,sql_client

from kafka_producer.config.Config import TABLE


class SQLOrmService(SQLClient):
    def __init__(self):
        super().__init__()

        self.__checkout_table=TABLE["checkout"]
        self.__metadata_table = TABLE["metadata"]
        self.__user_session_cart_table = TABLE["user_session_cart"]

    def insert_to_order_submit(self,request_body: dict):
        logging.info("Inserting to Order submit")
        try:
            if request_body:
                order_details = sql_client.get_record(self.__checkout_table,request_body["session_id"],request_body["uid"])
                cart_details= sql_client.get_cart(self.__user_session_cart_table,request_body["session_id"])
                if len(cart_details) == 0:
                    raise Exception ("cart does not have anything")
                print("yhh",order_details)
                if order_details:
                    logging.info("corresponding record found")
                    request_body["grand_total"]=json.loads(order_details[0]["checkout_details"])["grand_total"]
                    request_body["ldts"]= HelperUtils.get_timestamp()
                    print("hjh", request_body)
                    if request_body:
                        print("hjh",request_body)
                        if sql_client.insert(self.__metadata_table,request_body):
                            request_body["delvery_addrss_id"] = json.loads(order_details[0]["checkout_details"])["delvery_addrss_id"]
                            request_body["checkout_details"] = json.loads(order_details[0]["checkout_details"])
                            request_body["full_order"] = json.dumps(json.loads(order_details[0]["full_order"]))
                            request_body["cart_details"] = json.dumps(cart_details)
                            return request_body

        except Exception as e :
            logging.info("there is a issue")
            logging.error(e)


sql_orm_service= SQLOrmService()

