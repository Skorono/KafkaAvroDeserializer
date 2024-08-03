import os

from deserializers import KafkaAvroDeserializer


def read_response_from_file(file_name: str) -> bytes:
    with open(file_name, 'rb') as f:
        return f.read()


def main():
    deserializer = KafkaAvroDeserializer()
    deserializer.load_schema(os.path.join("schemes", "chart.avsc"))

    # message = re.sub(r'[\x00-\x1F\x7F]', '',  read_response_from_file(os.path.join("responses", "charts.txt"))).decode("utf-8"))
    message = read_response_from_file(os.path.join("responses", "charts.txt"))

    deserializer.deserialize(message)



if __name__ == "__main__":
    main()
