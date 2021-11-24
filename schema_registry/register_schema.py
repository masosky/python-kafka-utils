from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

sr_conf: dict = {'url': "http://localhost:8081"}
schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(sr_conf)

schema_str: str = '''
{
"namespace": "com.telefonica",
 "type": "record",
 "name": "Subject",
 "fields": [
     {"name": "name", "type": "string"}
 ]
}
'''
schema: Schema = Schema(schema_str=schema_str, schema_type="AVRO")

subject: str = "my-subject"
schema_id: int = schema_registry_client.register_schema(subject_name=subject, schema=schema)
print(f'Schema deployed {schema_id}')
