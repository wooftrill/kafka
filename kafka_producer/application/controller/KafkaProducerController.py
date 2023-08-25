import logging,json
from http import HTTPStatus
from dataclasses import asdict
from flask import jsonify, make_response,request
from kafka_producer.application.model.Error import Error
from kafka_producer.application.model.PaymentGwModel import PaymentGwModel
from kafka_producer.utils.HelperUtils import HelperUtils
from kafka_producer.application import app
from kafka_producer.application.service.kafkaProducerService import kafka_producer_service,KafkaProducerService
from functools import wraps

logging.getLogger().setLevel(logging.INFO)


class KafkaProducerController:
    def __init__(self,s: KafkaProducerService):
        self.service = s


kafka_producer_controller = KafkaProducerController(kafka_producer_service)


@app.get('/')
def welcome():
    return "Welcome to service"


def cors_preflight():
    # Respond to the OPTIONS preflight request with the necessary CORS headers
    response = make_response()
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'POST')
    return response


# Handle the OPTIONS request for /sign_in/ separately
app.add_url_rule('/send_to_kafka', view_func=cors_preflight, methods=['OPTIONS'])


def check_api_key(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        api_key = "Wt_opsKafka12if"
        if auth_header == f'Bearer {api_key}':
            return func(*args, **kwargs)
        else:
            return jsonify({"error": "Unauthorized"}, 401)
    return wrapper


@app.route('/send_to_kafka', methods=['POST'])
@check_api_key
def send_to_kafka():
    """

    :return:
    """
    try:
        logging.info("starting")
        request_json = request.get_json()
        print(request_json)
        if request_json is None:
            error = Error(message="no json body present", type=400, message_id=HTTPStatus.BAD_REQUEST)
            return make_response(jsonify(error, HTTPStatus.BAD_REQUEST))
        else:
            if HelperUtils.validator(request_json):
                session_id= request_json['session_id']
                uid=request_json['uid']
                pg_order_id = request_json['pg_order_id']
                order_id = request_json['order_id']
                signature = request_json['signature']
                req_body= asdict(PaymentGwModel(session_id,uid,pg_order_id,order_id,signature))
                print(req_body)
                if kafka_producer_controller.service.produce_to_topic(req_body):
                    return jsonify(True,200)
                else:
                    error = Error(message="Internal Server Error", type=500, message_id=HTTPStatus.INTERNAL_SERVER_ERROR)
                    return make_response(jsonify(error, HTTPStatus.INTERNAL_SERVER_ERROR))
            else:
                print(HelperUtils.validator(request_json))
                error = Error(message="Bad formatting of Body", type=400, message_id=HTTPStatus.BAD_REQUEST)
                return make_response(jsonify(error, HTTPStatus.BAD_REQUEST))

    except Exception as ex:
        logging.error(ex)


