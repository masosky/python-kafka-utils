from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError, ConsumeError
from confluent_kafka.serialization import StringDeserializer

topic: str = "regular-consumer"

consumer_conf = {'bootstrap.servers': "localhost:9092", 'key.deserializer': StringDeserializer('utf_8'),
                 'value.deserializer': StringDeserializer('utf_8'), 'group.id': "group-id",
                 "auto.offset.reset": "largest"}

# consumer_conf["auto.offset.reset"] = "earliest"

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([topic])

while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(timeout=-1)
        if msg is None:
            continue
        msg: Message
        value = msg.value()
        if value is not None:
            print(f'Key: {msg.key()} Value: {value} Offset: {msg.offset()} Partition: {msg.partition()}')
    except KeyDeserializationError as e:
        raise e
    except ValueDeserializationError as e:
        raise e
    except ConsumeError as e:
        raise e

consumer.close()
