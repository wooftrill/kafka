import logging,json
from http import HTTPStatus
from dataclasses import asdict
from flask import jsonify, make_response,request
from kafka_producer.application.model.Error import Error
from kafka_producer.application.model.PaymentGwModel import PaymentGwModel
from kafka_producer.utils.HelperUtils import HelperUtils
from kafka_producer.application import app
from kafka_producer.application.service.KafkaProducer import KafkaProducer,kafka_producer

logging.getLogger().setLevel(logging.INFO)


class KafkaProducerController:
    def __init__(self,s: KafkaProducer):
        self.service = s


kafka_producer_controller = KafkaProducerController(kafka_producer)

API_KEY="Wt_opsKafka12if"


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


@app.post('/send_to_kafka',endpoint='send_to_kafka')
@HelperUtils.check_api_key(API_KEY)
def send_to_kafka():
    """

    :return:
    """
    try:
        request_json = request.get_json()
        print(request_json)
        if request_json is None:
            error = Error(message="no json body present", type=400, message_id=HTTPStatus.BAD_REQUEST)
            return make_response(jsonify(error, HTTPStatus.BAD_REQUEST))
        else:
            if HelperUtils.validator(request_json):
                order_id= request_json['order_id']
                pg_order_id=request_json['pg_order_id']
                req_body= asdict(PaymentGwModel(order_id,pg_order_id))
                if kafka_producer_controller.service.producer(req_body):
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


