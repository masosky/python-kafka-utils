import _thread
import time
from logging import Logger
from typing import Optional

from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError, ConsumeError
from confluent_kafka.serialization import StringDeserializer

from logger.logger import get_logger


def start_consumer(threadName):
    logger: Logger = get_logger()
    logger.debug(f"{threadName}, {time.ctime(time.time())}")
    topic: str = "test"

    consumer_conf = {'bootstrap.servers': "localhost:9092",
                     'key.deserializer': StringDeserializer('utf_8'),
                     'value.deserializer': StringDeserializer('utf_8'),
                     'group.id': "test-group-id",
                     'enable.auto.commit': True,
                     "auto.offset.reset": "largest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])
    logger.debug(f"Subscribed top topic: {topic}")

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg: Optional[Message] = consumer.poll(timeout=-1)
            if msg is None:
                continue
            logger.debug(
                f'Key: {msg.key()} Value: {msg.value()} Offset: {msg.offset()} Partition: {msg.partition()}')
        except KeyDeserializationError as e:
            raise e
        except ValueDeserializationError as e:
            raise e
        except ConsumeError as e:
            raise e


try:
    _thread.start_new_thread(start_consumer, ("Thread-1",))
    _thread.start_new_thread(start_consumer, ("Thread-2",))
except:
    print("Error: unable to start thread")

while 1:
    pass
