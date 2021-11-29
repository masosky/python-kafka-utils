# Python Kafka Utils

## Presentation Review

### Control Center

http://localhost:9091

### Schema Registry Postman

[collection.json](Schema%20Registry.postman_collection.json)

### Multiple Consumers

[multiple_consumers.py](basics/multiple_consumers.py)

### Kafka Admin

[kafka_admin.py](basics/kafka_admin.py)

## Kafka Consuming / Producing

### String

#### Async

[kafka_string_producer.py](kafka_producer/string/kafka_string_producer.py)
[kafka_string_async_consumer.py](kafka_consumer/string/async/kafka_string_async_consumer.py)

#### Sync

[kafka_string_producer.py](kafka_producer/string/kafka_string_producer.py)
[kafka_string_sync_consumer.py](kafka_consumer/string/sync/kafka_string_sync_consumer.py)

### Json

[kafka_json_producer.py](kafka_producer/json/kafka_json_producer.py)
[kafka_json_consumer.py](kafka_consumer/json/kafka_json_consumer.py)

### Avro

[kafka_avro_producer.py](kafka_producer/avro/kafka_avro_producer.py)
[kafka_avro_consumer.py](kafka_consumer/avro/kafka_avro_consumer.py)

### Schema Registry

#### Publish Schemas

##### Avro

[register_avro_schema.py](schema_registry/avro/register_avro_schema.py)

##### Json

[register_json_schema.py](schema_registry/json/register_json_schema.py)

##### Protobuf

[register_protobuf_schema.py](schema_registry/protobuf/register_protobuf_schema.py)

#### Json

[kafka_schema_registry_json_schema_producer.py](kafka_producer/schema_registry/json_schema/kafka_schema_registry_json_schema_producer.py)
[kafka_json_schema_registry_consumer.py](kafka_consumer/schema_registry/json_schema/kafka_json_schema_registry_consumer.py)

#### Avro

[kafka_schema_registry_avro_producer.py](kafka_producer/schema_registry/avro/kafka_schema_registry_avro_producer.py)
[kafka_avro_schema_registry_consumer.py](kafka_consumer/schema_registry/avro/kafka_avro_schema_registry_consumer.py)

## Confluent Quickstart

https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#quickstart