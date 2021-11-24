from logging import Logger

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

from logger.logger import get_logger

logger: Logger = get_logger()
sr_conf: dict = {'url': "http://localhost:8081"}
schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(sr_conf)

file_object = open("person.avsc", "r")
schema_str: str = file_object.read()
file_object.close()
schema: Schema = Schema(schema_str=schema_str, schema_type="AVRO")

subject: str = "person"
schema_id: int = schema_registry_client.register_schema(subject_name=subject, schema=schema)
logger.debug(f'Schema deployed with id: {schema_id} and subject: {subject}')
