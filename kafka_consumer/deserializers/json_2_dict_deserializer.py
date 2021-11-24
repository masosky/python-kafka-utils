import json
from logging import Logger
from typing import Optional

from confluent_kafka.serialization import Deserializer, SerializationContext

from logger.logger import get_logger


class Json2DictDeserializer(Deserializer):

    def __init__(self, unicode: str = "utf-8"):
        self._unicode: str = unicode
        self._logger: Logger = get_logger()
        self._logger.debug("Json2DictDeserializer constructor")

    def __call__(self, value: bytes, context: SerializationContext) -> Optional[dict]:
        try:
            return json.loads(value.decode(self._unicode))
        except Exception:
            self._logger.exception("Error when trying to Deserialize String to Json")
            return None
