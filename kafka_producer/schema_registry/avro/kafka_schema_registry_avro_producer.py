import json
from logging import Logger

from confluent_kafka import SerializingProducer, KafkaError
from confluent_kafka.cimpl import Message, KafkaException
from confluent_kafka.error import ValueSerializationError, KeySerializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from logger.logger import get_logger

logger: Logger = get_logger()
logger.debug("Starting Kafka String Producer")

file_object = open("cercle.avsc", "r")
schema_str: str = file_object.read()
file_object.close()
sr_conf: dict = {'url': "http://localhost:8081"}
schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(sr_conf)
conf = {'bootstrap.servers': 'localhost:9092',
        # 'debug': 'all',
        'debug': 'msg',
        'compression.codec': 'lz4',
        'key.serializer': StringSerializer(codec='utf_8'),
        'value.serializer': AvroSerializer(schema_registry_client=schema_registry_client, schema_str=schema_str)
        }
producer: SerializingProducer = SerializingProducer(conf)
topic = "cercle-avro-topic"


def delivery_callback(err: KafkaError, msg: Message):
    if err:
        logger.error(f'Message failed delivery: {err}')
    else:
        logger.debug(
            f'Message delivered to topic={msg.topic()} partition={msg.partition()}  offset={msg.offset()}\n')


try:
    key = None
    value = {"radius": 2.0, "diameter": 4.0}
    # message_bytes: bytes = json.dumps(value).encode("utf-8")
    # producer.produce(topic, key=key, value=message_bytes, on_delivery=delivery_callback)
    producer.produce(topic, key=key, value=value, on_delivery=delivery_callback)
    producer.flush(5)
except BufferError as e:
    raise e
except KeySerializationError as e:
    raise e
except ValueSerializationError as e:
    raise e
except KafkaException as e:
    raise e
