import json
import os
import logging
import time

from kafka_producer.utils.SQLUtils import SQLUtils
from kafka_producer.utils.HelperUtils import HelperUtils
from sqlalchemy.sql import text
from sqlalchemy.exc import *
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer,Table,String,MetaData


class SQLClient:

    def __init__(self) ->None:
        self.sql_conn = "mysql+pymysql://WTrw:WoofandTrill%4012@localhost:3306/external"
        self.engine = create_engine(self.sql_conn)

        pass

    def get_record(self,table_name,session_id,uid):
        __min_available = 1
        max_retries = 3
        retries = 0
        logging.info("checking if table exist!!...")
        while retries < max_retries:
            try:
                inspect_db = sa.inspect(self.engine)
                is_exist = inspect_db.dialect.has_table(self.engine.connect(), table_name, schema="external")
                if not is_exist:
                    raise NoSuchTableError
                else:
                    curr_session = sessionmaker(bind=self.engine)
                    session = curr_session()
                    query = SQLUtils.get_order(table_name, session_id, uid)
                    print(query)
                    cart_list = []
                    response = session.execute(text(query))
                    session.close()
                    logging.info("session closed")
                    for res in response:
                        cart_list.append(res)
                    return HelperUtils.tupple_to_dict(cart_list,
                                                      ["checkout_details", "full_order"])
            except OperationalError as e:
                logging.error("Error: connection issue {}".format(e))
                retries += 1
                print("in loop")
                time.sleep(1)
            except Exception as ex:
                logging.error("An exception occurred:{}".format(ex))
                raise ex

        raise Exception("Could not perform database eration after {} retries".format(max_retries))

    def insert(self, table_name: str, sql_model: dict):
        try:
            logging.info("checking if table exist!!...")
            inspect_db = sa.inspect(self.engine)
            is_exist = inspect_db.dialect.has_table(self.engine.connect(), table_name, schema="external")
            if not is_exist:
                raise NoSuchTableError
            else:
                curr_session = sessionmaker(bind=self.engine)
                session = curr_session()
                values = tuple(sql_model.values())
                query = SQLUtils.insert_query(table_name, values)
                response = session.execute(text(query))
                if response.__dict__['rowcount'] > 0:
                    logging.info("row inserted!..")
                    return True
                raise SQLAlchemyError("Error!...no row affected")
        except DataError as e:
            session.rollback()
            logging.error(f"Insert failed: {e}")
        except Exception as ex:
            session.rollback()
            logging.error(f"issue with query:{ex}")
        finally:
            session.commit()
            session.close()
            logging.info("session closed")


sql_client = SQLClient()

#k=sql_client.get_record("tbl_checkout","ghsgdhsh7873673hwgdhll-jkj-","36141bb3a7ccccb7c733e7bff6b697abe84da8c6")

#t=json.loads(k[0]["checkout_details"])["net_total"]
#print(t)