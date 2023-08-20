import json,logging

from jsonschema import validate
from kafka import KafkaProducer
from functools import wraps
from flask import request, jsonify

logging.getLogger().setLevel(logging.INFO)


class HelperUtils:
    def __init__(self):
        pass

    set_tl_schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "pg_order_id": {"type": "string"},
        },
    }

    @staticmethod
    def validator(request_body):
        try:
            validate(instance=request_body, schema=HelperUtils.set_tl_schema)
            return True
        except Exception as ex:
            print(ex)

    @staticmethod
    def tupple_to_dict(sql_response_list: list, keys: list):
        # keys = ["session_id","cart_id", "item_id", "count","is_active"]
        json_list = []
        print(sql_response_list)
        for tpl in sql_response_list:
            json_dict = {}
            for i in range(len(tpl)):
                json_dict[keys[i]] = tpl[i]
            json_list.append(json_dict)
        return json_list

    @staticmethod
    def kafka_producer_client(bootstrap_servers):
        return KafkaProducer(bootstrap_servers=bootstrap_servers, acks=1)

    @staticmethod
    def check_api_key(api_key):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                auth_header = request.headers.get('Authorization')

                # Check if the Authorization header is present and has the correct API key
                if auth_header == f'Bearer {api_key}':
                    print(func(*args, **kwargs))
                    return func(*args, **kwargs)
                else:
                    return jsonify({"error": "Unauthorized"}, 401)

            return wrapper

        return decorator






