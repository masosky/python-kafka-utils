import io
from logging import Logger

import avro.io
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
topic = "car-avro-topic"


def delivery_callback(err: KafkaError, msg: Message):
    if err:
        logger.error(f'Message failed delivery: {err}')
    else:
        logger.debug(
            f'Message delivered to topic={msg.topic()} partition={msg.partition()}  offset={msg.offset()}\n')


def encode_data(value: dict) -> bytes:
    schema_str: str = open("car.avsc", 'r').read()
    schema = avro.schema.parse(schema_str)
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    raw_bytes = bytes_writer.getvalue()
    bytes_writer.flush()
    bytes_writer.seek(0)
    return raw_bytes


try:
    key = None
    value = {"brand": "Mercedes-Benz", "cv": 180.6}
    raw_bytes = encode_data(value)
    producer.produce(topic, key=key, value=raw_bytes, on_delivery=delivery_callback)
    producer.flush(5)
except BufferError as e:
    raise e
except KeySerializationError as e:
    raise e
except ValueSerializationError as e:
    raise e
except KafkaException as e:
    raise e
