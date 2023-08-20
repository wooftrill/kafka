import config
import configparser
from apiflask import APIFlask
from flask_cors import CORS

app = APIFlask(__name__,title="kafka_api_wt",version='1.0.0',spec_path='/spec')
cors = CORS(app)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

from kafka_producer.application.controller.KafkaProducerController import KafkaProducerController