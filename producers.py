import os

import avro
from avro.io import DatumWriter, BinaryEncoder
import io

from avro.schema import Schema
from kafka import KafkaProducer

logs_data = {
    "logs": [
        {
            "logType": "swap",
            "blockNumber": 20450862,
            "blockTimestamp": 1722722399000,
            "txnHash": "0x33edf28c5378a089d6608a11b02908c073d28de34bbb07dc5a5a7795ea66e3a0",
            "maker": "0x5B9FEa929F682972BCe369950acc0123c0493A47",
            "makerScreener": {
                "buys": 12,
                "sells": 4,
                "volumeUsdBuy": 3761.37,
                "volumeUsdSell": 2474.91,
                "amountBuy": "25043.55",
                "amountSell": "16813.76",
                "balanceAmount": "8229.78",
                "balancePercentage": 32.86,
                "firstSwap": 1722358043000,
                "new": False
            },
            "logIndex": 108,
            "txnType": "sell",
            "priceUsd": "0.1438",
            "priceNative": "0.00004940",
            "volumeUsd": "428.81",
            "amount0": "2988.00",
            "amount1": "0.1472"
        },
        {
            "logType": "swap",
            "blockNumber": 20450857,
            "blockTimestamp": 1722722339000,
            "txnHash": "0xa6de0728df7fe5257bec5c7e89e0e26eeef65a7e3ab4fef491f595a9b0e08a55",
            "maker": "0xBb8b8ff0FeDB45Bc0c9e0233e9D04557B336a7Eb",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 2747.28,
                "volumeUsdSell": 0,
                "amountBuy": "19084.32",
                "amountSell": "0",
                "balanceAmount": "19084.32",
                "balancePercentage": 100,
                "firstSwap": 1722722279000,
                "new": False
            },
            "logIndex": 131,
            "txnType": "buy",
            "priceUsd": "0.1440",
            "priceNative": "0.00004946",
            "volumeUsd": "917.23",
            "amount0": "6357.25",
            "amount1": "0.3149"
        },
        {
            "logType": "swap",
            "blockNumber": 20450852,
            "blockTimestamp": 1722722279000,
            "txnHash": "0xe47dcc9adc791cdf36f7ad460894e30e71aeb69823eadcce251b2919f6c0ce06",
            "maker": "0xBb8b8ff0FeDB45Bc0c9e0233e9D04557B336a7Eb",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 2747.28,
                "volumeUsdSell": 0,
                "amountBuy": "19084.32",
                "amountSell": "0",
                "balanceAmount": "19084.32",
                "balancePercentage": 100,
                "firstSwap": 1722722279000,
                "new": True
            },
            "logIndex": 166,
            "txnType": "buy",
            "priceUsd": "0.1437",
            "priceNative": "0.00004933",
            "volumeUsd": "1830.050",
            "amount0": "12727.060",
            "amount1": "0.6282"
        },
        {
            "logType": "swap",
            "blockNumber": 20450850,
            "blockTimestamp": 1722722255000,
            "txnHash": "0x2f46d56625fbe88988285a57a04c2ffc9c07631df59d26494a2538f815053c76",
            "maker": "0x5701477834859bdF82324c4e50AcD2b345287b75",
            "makerScreener": {
                "buys": 0,
                "sells": 14,
                "volumeUsdBuy": 0,
                "volumeUsdSell": 20182.64,
                "amountBuy": "0",
                "amountSell": "126937.50",
                "balanceAmount": None,
                "balancePercentage": None,
                "firstSwap": 1722634463000,
                "new": False
            },
            "logIndex": 96,
            "txnType": "sell",
            "priceUsd": "0.1429",
            "priceNative": "0.00004908",
            "volumeUsd": "1428.53",
            "amount0": "10000.00",
            "amount1": "0.4903"
        },
        {
            "logType": "swap",
            "blockNumber": 20450847,
            "blockTimestamp": 1722722219000,
            "txnHash": "0x1e303b57a8ae6814314e9f41c2ac92fd971824997e0242ee5dbf2f18e88fb0bc",
            "maker": "0xa1230991Fc72c77BCc4923bcAF30974A2ee6803f",
            "makerScreener": {
                "buys": 4,
                "sells": 3,
                "volumeUsdBuy": 739.25,
                "volumeUsdSell": 612.18,
                "amountBuy": "5092.69",
                "amountSell": "3753.18",
                "balanceAmount": "1339.51",
                "balancePercentage": 26.3,
                "firstSwap": 1722402155000,
                "new": False
            },
            "logIndex": 207,
            "txnType": "sell",
            "priceUsd": "0.1435",
            "priceNative": "0.00004928",
            "volumeUsd": "71.57",
            "amount0": "500.00",
            "amount1": "0.02456"
        },
        {
            "logType": "swap",
            "blockNumber": 20450846,
            "blockTimestamp": 1722722207000,
            "txnHash": "0x121e3ab57c14a29350fc580984d984116d1007ed0bc21b56c74e40784e207d6c",
            "maker": "0x5645776618bB3642E93Dd4C8a3Db5283ee7b82E7",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 24.68,
                "volumeUsdSell": 0,
                "amountBuy": "171.060",
                "amountSell": "0",
                "balanceAmount": "171.060",
                "balancePercentage": 100,
                "firstSwap": 1722722111000,
                "new": False
            },
            "logIndex": 352,
            "txnType": "buy",
            "priceUsd": "0.1435",
            "priceNative": "0.00004929",
            "volumeUsd": "12.24",
            "amount0": "85.030",
            "amount1": "0.004204"
        },
        {
            "logType": "swap",
            "blockNumber": 20450846,
            "blockTimestamp": 1722722207000,
            "txnHash": "0x38967a9660569883db67424660e440c7cf31b4d39c852d570e7a1ef334fa04f2",
            "maker": "0xD2A7676D621A656051950D87517853B496210655",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 24.56,
                "volumeUsdSell": 0,
                "amountBuy": "170.22",
                "amountSell": "0",
                "balanceAmount": "170.22",
                "balancePercentage": 100,
                "firstSwap": 1722722111000,
                "new": False
            },
            "logIndex": 337,
            "txnType": "buy",
            "priceUsd": "0.1435",
            "priceNative": "0.00004929",
            "volumeUsd": "12.87",
            "amount0": "89.44",
            "amount1": "0.004421"
        },
        {
            "logType": "swap",
            "blockNumber": 20450844,
            "blockTimestamp": 1722722183000,
            "txnHash": "0x914550752a05b49f44461f0cfe947e928654ef9d7963154287896df7f405c0d9",
            "maker": "0x678E01914438355375DF6A8f90eF737203aC8819",
            "makerScreener": {
                "buys": 1,
                "sells": 4,
                "volumeUsdBuy": 65.08,
                "volumeUsdSell": 722.75,
                "amountBuy": "4799.89",
                "amountSell": "4799.89",
                "balanceAmount": "0",
                "balancePercentage": 0,
                "firstSwap": 1722215483000,
                "new": False
            },
            "logIndex": 85,
            "txnType": "sell",
            "priceUsd": "0.1434",
            "priceNative": "0.00004928",
            "volumeUsd": "193.17",
            "amount0": "1349.97",
            "amount1": "0.06635"
        },
        {
            "logType": "swap",
            "blockNumber": 20450844,
            "blockTimestamp": 1722722183000,
            "txnHash": "0xad2e3dee5a69e0576ad8029ff19f7132f36a5c89abf2ed149ab6851133b68bc3",
            "maker": "0x596158Bc00e290f0227c9cA5D7232B6dB565Cd9b",
            "makerScreener": {
                "buys": 3,
                "sells": 3,
                "volumeUsdBuy": 952.23,
                "volumeUsdSell": 493.11,
                "amountBuy": "8613.67",
                "amountSell": "4075.00",
                "balanceAmount": "4538.67",
                "balancePercentage": 52.69,
                "firstSwap": 1722312011000,
                "new": False
            },
            "logIndex": 54,
            "txnType": "sell",
            "priceUsd": "0.1435",
            "priceNative": "0.00004931",
            "volumeUsd": "199.72",
            "amount0": "1395.00",
            "amount1": "0.06860"
        },
        {
            "logType": "swap",
            "blockNumber": 20450841,
            "blockTimestamp": 1722722147000,
            "txnHash": "0x5be8e67e3e4d02d6ea57ae1088b6553dc9b449dbb9371e007ecb19c61825b7c5",
            "maker": "0xE266E36b61FE5f4338a87a0Ef0597C77633cEE04",
            "makerScreener": {
                "buys": 8,
                "sells": 1,
                "volumeUsdBuy": 1365.15,
                "volumeUsdSell": 167.97,
                "amountBuy": "97865.53",
                "amountSell": "37717.57",
                "balanceAmount": "60147.95",
                "balancePercentage": 61.45,
                "firstSwap": 1722140999000,
                "new": False
            },
            "logIndex": 181,
            "txnType": "buy",
            "priceUsd": "0.1436",
            "priceNative": "0.00004934",
            "volumeUsd": "122.27",
            "amount0": "848.78",
            "amount1": "0.04200"
        },
        {
            "logType": "swap",
            "blockNumber": 20450840,
            "blockTimestamp": 1722722135000,
            "txnHash": "0x97a89113f9c77e9ab1f25ac1554632b5353c2677ea779c86c9aba2dd1836c8fc",
            "maker": "0x242dAc64a1671c38e826DD4c86cCe04900cE3824",
            "makerScreener": {
                "buys": 0,
                "sells": 11,
                "volumeUsdBuy": 0,
                "volumeUsdSell": 16049.62,
                "amountBuy": "0",
                "amountSell": "99500.00",
                "balanceAmount": None,
                "balancePercentage": None,
                "firstSwap": 1722632603000,
                "new": False
            },
            "logIndex": 419,
            "txnType": "sell",
            "priceUsd": "0.1436",
            "priceNative": "0.00004932",
            "volumeUsd": "1435.36",
            "amount0": "10000.00",
            "amount1": "0.4927"
        },
        {
            "logType": "swap",
            "blockNumber": 20450838,
            "blockTimestamp": 1722722111000,
            "txnHash": "0x9ff3e04d8cce2f224fb3f1492ebe1858de9e6ee44c43a1d2c0efa76b72f94e9d",
            "maker": "0x5645776618bB3642E93Dd4C8a3Db5283ee7b82E7",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 24.68,
                "volumeUsdSell": 0,
                "amountBuy": "171.060",
                "amountSell": "0",
                "balanceAmount": "171.060",
                "balancePercentage": 100,
                "firstSwap": 1722722111000,
                "new": True
            },
            "logIndex": 370,
            "txnType": "buy",
            "priceUsd": "0.1442",
            "priceNative": "0.00004952",
            "volumeUsd": "12.44",
            "amount0": "86.020",
            "amount1": "0.004273"
        },
        {
            "logType": "swap",
            "blockNumber": 20450838,
            "blockTimestamp": 1722722111000,
            "txnHash": "0xfbbbb2991a2ba782d51d0c2c00a0b1d5d54b13654395d581de334626f7fa3476",
            "maker": "0xD2A7676D621A656051950D87517853B496210655",
            "makerScreener": {
                "buys": 2,
                "sells": 0,
                "volumeUsdBuy": 24.56,
                "volumeUsdSell": 0,
                "amountBuy": "170.22",
                "amountSell": "0",
                "balanceAmount": "170.22",
                "balancePercentage": 100,
                "firstSwap": 1722722111000,
                "new": True
            },
            "logIndex": 365,
            "txnType": "buy",
            "priceUsd": "0.1442",
            "priceNative": "0.00004952",
            "volumeUsd": "11.68",
            "amount0": "80.78",
            "amount1": "0.004012"
        }
    ]
}

bootstrap_servers = ['localhost:9092']
topic_name = 'chart_topic'


class KafkaProducer:
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, )

    def produce(self, jmessage: dict, message_schema: Schema):
        buf = io.BytesIO()
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(message_schema)
        writer.write(jmessage, encoder)
        buf.seek(0)
        message_data = (buf.read())

        key = None

        headers = []

        self.producer.send(topic_name,
                           message_data,
                           key,
                           headers)
        self.producer.flush()


schema = avro.schema.parse(open(os.path.join("schemes", "logs.avsc"), "r").read())
KafkaProducer = KafkaProducer()
KafkaProducer.produce(logs_data, schema)
