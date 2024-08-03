import io
import json

import avro.schema
from avro.io import DatumReader, BinaryDecoder
from avro.schema import Schema


class KafkaAvroDeserializer:
    schema: Schema
    reader: DatumReader

    def load_schema(self, schema_path: str):
        self.schema = avro.schema.parse(open(schema_path).read())
        self.reader = DatumReader(self.schema)

    def deserialize(self, bytes_message: bytes) -> json:
        message_bytes = io.BytesIO(bytes_message)
        decoder = BinaryDecoder(message_bytes)

        return json.load(self.reader.read(decoder))
