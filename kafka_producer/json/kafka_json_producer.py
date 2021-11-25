import json
from logging import Logger

from confluent_kafka import SerializingProducer, KafkaError
from confluent_kafka.cimpl import Message, KafkaException
from confluent_kafka.error import ValueSerializationError, KeySerializationError

from logger.logger import get_logger

logger: Logger = get_logger()
logger.debug("Starting Kafka String Producer")
conf = {'bootstrap.servers': 'localhost:9092',
        # 'debug': 'all',
        'debug': 'msg',
        'compression.codec': 'lz4'
        }
producer: SerializingProducer = SerializingProducer(conf)
topic = "json-topic"


def delivery_callback(err: KafkaError, msg: Message):
    if err:
        logger.error(f'Message failed delivery: {err}')
    else:
        logger.debug(
            f'Message delivered to topic={msg.topic()} partition={msg.partition()}  offset={msg.offset()}\n')


try:
    key = None
    value = {"a": 1, "b": 2}
    message_bytes: bytes = json.dumps(value).encode("utf-8")
    producer.produce(topic, key=key, value=message_bytes, on_delivery=delivery_callback)
    producer.flush(5)
except BufferError as e:
    raise e
except KeySerializationError as e:
    raise e
except ValueSerializationError as e:
    raise e
except KafkaException as e:
    raise e
