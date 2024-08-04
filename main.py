import os
import cloudscraper
from deserializers import KafkaAvroDeserializer


def main():
    deserializer = KafkaAvroDeserializer()
    deserializer.load_schema(os.path.join("schemes", "chart.avsc"))

    scraper = cloudscraper.create_scraper()
    message = scraper.get("https://io.dexscreener.com/dex/chart/amm/v3/uniswap/bars/ethereum"
                         "/0x3885fbe4CD8aeD7b7e9625923927Fa1CE30662A3?abn=20454676&res=15&cb=2&q"
                          "=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
                         ).text.encode("utf-8")

    #message = open(os.path.join('responses', 'charts.txt'), 'rb').read()

    message = message.replace(b'\x02\x04', b'\x04')
    message = message.replace(b'\xef\xbf\xbd', b'\x80')
    print(message)

    deserializer.deserialize(message)


if __name__ == "__main__":
    main()
