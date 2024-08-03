import os

from deserializers import KafkaAvroDeserializer


def read_response_from_file(file_name: str) -> bytes:
    with open(file_name, 'rb') as f:
        return f.read()


def main():
    deserializer = KafkaAvroDeserializer()
    deserializer.load_schema(os.path.join("schemes", "logs.avsc"))

    message = read_response_from_file(os.path.join("schemes", "logs.avsc"))
    deserializer.deserialize(message)


if __name__ == "__main__":
    main()
