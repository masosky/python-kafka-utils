from logging import Logger
from typing import Optional

from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError, ConsumeError
from confluent_kafka.serialization import StringDeserializer

from kafka_consumer.deserializers.json_2_dict_deserializer import Json2DictDeserializer
from logger.logger import get_logger

logger: Logger = get_logger()
topic: str = "json-topic"

consumer_conf = {'bootstrap.servers': "localhost:9092",
                 'key.deserializer': StringDeserializer('utf_8'),
                 'value.deserializer': Json2DictDeserializer(unicode='utf_8'),
                 'group.id': "group-id-json-topic",
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
