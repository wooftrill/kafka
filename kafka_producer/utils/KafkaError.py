import  os
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
class KafkaError:
    NoBrokersAvailable = NoBrokersAvailable
    KafkaTimeoutError = KafkaTimeoutError
    KafkaError = KafkaError


kafka_error = KafkaError()
