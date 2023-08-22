import  os
import logging

logging.getLogger().setLevel(logging.INFO)


class SQLUtils:
    """
    class for generating query
    """
    def __init__(self) -> None:
        pass

    @staticmethod
    def insert_query(table_name: str,args):
        return f"INSERT INTO external.{table_name} VALUES {args};"


    @staticmethod
    def get_order(table_name: str,session_id: str, uid: str):
        return f"SELECT checkout_details,full_order from {table_name} where session_id='{session_id}' and uid ='{uid}' and status= 0;"
