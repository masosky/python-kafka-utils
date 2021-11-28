import io
from logging import Logger
from typing import Optional

import avro.io
import avro.schema
from confluent_kafka.serialization import Deserializer, SerializationContext

from logger.logger import get_logger


class AvroDeserializer(Deserializer):

    def __init__(self, schema_str: str):
        self._schema = avro.schema.parse(schema_str)
        self._logger: Logger = get_logger()
        self._logger.debug("AvroDeserializer constructor")

    def __call__(self, value: bytes, context: SerializationContext) -> Optional[object]:
        try:
            bytes_reader = io.BytesIO(value)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(self._schema)
            return reader.read(decoder)
        except Exception:
            self._logger.exception("Error when trying to Deserialize Avro")
            return None
