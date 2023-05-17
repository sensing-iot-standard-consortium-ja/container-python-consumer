import os
import json
import struct
from functools import cache
from typing import Dict, Tuple, List, Callable

import requests
from confluent_kafka import Consumer, Producer

from log import logger
from stdclasses import SchemaField, RawContainer

iot_schema_registory = os.getenv("IOT_SCHEMA_REGISTORY", "https://registry.iotbase.in")
logger.info(f"IOT_SCHEMA_REGISTORY: {iot_schema_registory}")

kafka_broker = os.getenv("KAFKA_BROKER", "websensor.iotbase.in:19092")
logger.info(f"KAFKA_BROKER: {kafka_broker}")

subscribe_topic_txt = os.getenv("SUBSCRIBE_TOPIC", "")
subscribe_topics = subscribe_topic_txt.split(",")
logger.info(f"SUBSCRIBE_TOPIC: {subscribe_topics}")


class AppException(Exception):
    pass


def get_data_id_length(data_index: bytes) -> bytes:
    length_dict: Dict[bytes, bytes] = {
        b"\x00": (16).to_bytes(1, "big"),
        b"\x01": (10).to_bytes(1, "big"),
        b"\x02": (14).to_bytes(1, "big"),
        b"\x03": (15).to_bytes(1, "big"),
        b"\x04": (16).to_bytes(1, "big"),
        b"\x05": (0).to_bytes(1, "big"),
    }
    return length_dict[data_index]


def parse(data: bytes):
    # コンテナ共通の処理
    # ContainerType
    if len(data) < 2:
        raise Exception("invalid data length")
    type = data[0:2]
    # ContainerLength
    length = data[2:4]
    # DataIndex
    data_index = data[4:5]
    # DataId
    data_id_length_b: bytes = get_data_id_length(data_index)
    data_id_offset = 5
    data_id_length = int.from_bytes(data_id_length_b, "big")
    data_id = data[data_id_offset : data_id_offset + data_id_length]
    # AttrPair
    # Payload
    payload_offset = data_id_offset + data_id_length
    payload = data[payload_offset:]

    return RawContainer(type, length, data_index, data_id_length, data_id, payload)


@cache
def retrive(key: Tuple[bytes, bytes]) -> Dict:
    """
    ContainerのDataIndex/DataIdを基にスキーマファイルを取得＆キャッシュする
    キャッシュクリア
    """
    data_index_b, data_id_b = key
    hostname: str = iot_schema_registory
    data_index: int = int.from_bytes(data_index_b, "big")
    data_id: str = data_id_b.hex()
    url: str = f"{hostname}/registry/repo/{data_index}/{data_id}"

    res = requests.get(url)
    if res.status_code == 200:
        logger.info("fetched")
        return json.loads(res.text)
    else:
        logger.error("error occured")
        return


def convert(type: str, payload: bytes, tags):
    """コンテナのPayloadをスキーマに従って構造化する"""
    try:
        endian = "little" if "isLittleEndian" in tags else "big"
        if type in ["bytes"]:
            return payload
        if type in ["uint", "u8", "u16", "u32", "u64"]:
            return int.from_bytes(payload, endian, signed=False)
        if type in ["int", "i8", "i16", "i32", "i64"]:
            return int.from_bytes(payload, endian, signed=True)
        if type in ["float", "float32", "float64"]:
            if len(payload) not in [2, 4, 8]:
                raise AppException("Invalid Payload Length")
            if len(payload) == 2 & endian == "big":
                return struct.unpack(">e", payload)[0]
            if len(payload) == 2 & endian == "little":
                return struct.unpack("<e", payload)[0]
            if len(payload) == 4 & endian == "big":
                return struct.unpack(">f", payload)[0]
            if len(payload) == 4 & endian == "little":
                return struct.unpack("<f", payload)[0]
            if len(payload) == 8 & endian == "big":
                return struct.unpack(">d", payload)[0]
            if len(payload) == 8 & endian == "little":
                return struct.unpack("<d", payload)[0]
    except Exception as e:
        logger.error("error occured in extract")


def extract(payload: bytes, schema) -> List[SchemaField]:
    """コンテナのPayloadをスキーマに従って構造化する"""
    fields = []
    for field in schema.get("fields", []):
        name, pos, length, tags, type = (
            field["name"],
            field["pos"],
            field["length"],
            field["tags"],
            field["type"],
        )
        piece = payload[pos : pos + length]
        data = convert(type, piece, tags)
        fields.append(SchemaField(name, type, pos, length, tags, piece, data))
    return fields


from basic_process import basic_keyvalue
from aandd import user_process_aandd_acce
from aandd import user_process_aandd_bloodpressure
from aandd import user_process_aandd_temp
from aandd import user_process_aandd_weight
from aandd import user_process_aandd_summary

user_function: Dict[Tuple[bytes, bytes], Callable] = {
    (b"\x03", b"498104615159700"): user_process_aandd_summary,
    (b"\x03", b"498104615159701"): user_process_aandd_acce,
    (b"\x03", b"498104602619200"): user_process_aandd_bloodpressure,
    (b"\x03", b"498104630619500"): user_process_aandd_temp,
    (b"\x03", b"498104614337000"): user_process_aandd_weight,
}


def main():
    # コンテナを受信する consumer
    consumer = Consumer(
        {
            "group.id": "hogehoge",
            "auto.offset.reset": "earliest",
            "bootstrap.servers": kafka_broker,
        }
    )
    consumer.subscribe(subscribe_topics)

    # jsonをProduceする producer
    producer = Producer({"bootstrap.servers": kafka_broker})

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("kafka error")
            continue
        try:
            c: RawContainer = parse(msg.value())
            schema: json = retrive((c.data_index, c.data_id))
            structured: List[SchemaField] = extract(c.payload, schema)

            user_process = user_function.get((c.data_index, c.data_id), basic_keyvalue)
            items: List = user_process(structured)
            logger.info(f"topic:{msg.topic()} offset:{msg.offset()} proessed")

            # 処理したjsonを出力
            topic = f"json_{msg.topic()[10:]}"  # container_ を除去
            for item in items:
                producer.produce(topic, json.dumps(item))
            producer.flush()
        except:
            logger.error(f"topic:{msg.topic()} offset:{msg.offset()} error!!")


if __name__ == "__main__":
    try:
        main()
    except (InterruptedError):
        pass
