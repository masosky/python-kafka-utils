from logging import Logger
from typing import Optional

from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError, ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from logger.logger import get_logger

logger: Logger = get_logger()
topic: str = "cercle-avro-topic"
sr_conf: dict = {'url': "http://localhost:8081"}
schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(sr_conf)

consumer_conf = {'bootstrap.servers': "localhost:9092",
                 'key.deserializer': StringDeserializer('utf_8'),
                 'value.deserializer': AvroDeserializer(schema_registry_client=schema_registry_client),
                 'group.id': "group-id-cercle-avro-topic",
                 "auto.offset.reset": "largest"}

# consumer_conf["auto.offset.reset"] = "earliest"

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([topic])
logger.debug(f"Subscribed top topic: {topic}")

while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg: Optional[Message] = consumer.poll(timeout=-1)
        if msg is None:
            continue
        logger.debug(f'Key: {msg.key()} Value: {msg.value()} Offset: {msg.offset()} Partition: {msg.partition()}')
    except KeyDeserializationError as e:
        raise e
    except ValueDeserializationError as e:
        raise e
    except ConsumeError as e:
        raise e
