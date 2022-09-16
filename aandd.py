from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List, Any
from main import SchemaField


def _decode_aandd_time(_byte_data: bytes) -> datetime:
    _tzinfo = timezone(timedelta(hours=9))
    _data: int = int.from_bytes(_byte_data, "big", signed=False)
    _year = ((_data & 0b111) << 3) + (_data >> 29) + 2005
    _month = ((_data & 0x1E000000) >> 25) + 1
    _day = (_data & 0x1F00000) >> 20
    _hour = (_data & 0xF8000) >> 15
    _minute = (_data & 0x7E00) >> 9
    _second = (_data & 0x1F8) >> 3
    return datetime(_year, _month, _day, _hour, _minute, _second, tzinfo=_tzinfo)


def find(items, name):
    """プロパティ.名前 をもつitemの検索"""
    return next(field for field in items if field.name == name)


def user_process_aandd_acce(structured: List[SchemaField]):
    public_address: str = find(structured, "public_address").data.hex()
    weared: int = find(structured, "weared").data[0]
    timestamp_b: bytes = find(structured, "timestamp").data
    timestamp_base: datetime = _decode_aandd_time(timestamp_b)
    acce: bytes = find(structured, "acce").data
    # 単位の例
    acce_st = find(structured, "acce_resolution")
    acce_resl: float = acce_st.data * float(acce_st.tags["unit"])
    items = []
    for i, base in enumerate(range(0, 480, 3)):
        timestamp = timestamp_base + timedelta(milliseconds=50 * i)
        ax = (acce[base + 0] - 128) * acce_resl
        ay = (acce[base + 1] - 128) * acce_resl
        az = (acce[base + 2] - 128) * acce_resl
        items.append(
            {
                "timestamp": int(timestamp.timestamp() * 1000),
                "public_address": public_address,
                "weared": weared,
                "ax": ax,
                "ay": ay,
                "az": az,
            }
        )
    return items


def user_process_aandd_bloodpressure(structured: List[SchemaField]):
    sys: int = find(structured, "sys").data
    dia: int = find(structured, "dia").data
    map: int = find(structured, "map").data
    pul: int = find(structured, "pul").data
    year: int = find(structured, "year").data
    month: int = find(structured, "month").data
    day: int = find(structured, "month").data
    hour: int = find(structured, "hour").data
    minute: int = find(structured, "minute").data
    second: int = find(structured, "second").data
    timestamp: datetime = datetime(
        year, month, day, hour, minute, second, tzinfo=_tzinfo
    )
    return [
        {
            "sys": sys,
            "dia": dia,
            "map": map,
            "pul": pul,
            "timestamp": int(timestamp.timestamp() * 1000),
        }
    ]


def user_process_aandd_temp(structured: List[SchemaField]):
    temp_mantissa: int = find(structured, "temp_mantissa").data
    temp_exp: int = find(structured, "temp_exp").data
    temp: float = temp_mantissa * (10**temp_exp)
    year: int = find(structured, "year").data
    month: int = find(structured, "month").data
    day: int = find(structured, "month").data
    hour: int = find(structured, "hour").data
    minute: int = find(structured, "minute").data
    second: int = find(structured, "second").data
    timestamp: datetime = datetime(
        year, month, day, hour, minute, second, tzinfo=_tzinfo
    )
    return [{"temp": temp, "timestamp": int(timestamp.timestamp() * 1000)}]


def user_process_aandd_weight(structured: List[SchemaField]):
    weight: float = find(structured, "weight").data / 200
    year: int = find(structured, "year").data
    month: int = find(structured, "month").data
    day: int = find(structured, "month").data
    hour: int = find(structured, "hour").data
    minute: int = find(structured, "minute").data
    second: int = find(structured, "second").data

    timestamp: datetime = datetime(
        year, month, day, hour, minute, second, tzinfo=_tzinfo
    )
    return [{"weight": weight, "timestamp": int(timestamp.timestamp() * 1000)}]


def user_process_aandd_summary(structured: List[SchemaField]):
    count = find(structured, "count").data
    payload = find(structured, "summary").data

    items = []
    for i in range(count):
        offset = i * 12
        timestamp = _decode_aandd_time(payload[offset + 0 : offset + 4])
        # temp = payload[offset + 4:offset + 5]
        walkcount = int.from_bytes(payload[offset + 5 : offset + 6], "big")
        # payload[offset + 6:offset + 7]
        # payload[offset + 7:offset + 8]
        kcal = int.from_bytes(payload[offset + 8 : offset + 9], "big")
        sleep_stat = int.from_bytes(payload[offset + 9 : offset + 10], "big")
        sleep_hour = int.from_bytes(payload[offset + 10 : offset + 11], "big")
        sleep_min = int.from_bytes(payload[offset + 11 : offset + 12], "big")
        items.append(
            {
                "timestamp": int(timestamp.timestamp()),
                "walkcount": walkcount,
                "kcal": kcal,
                "sleep_hour": sleep_hour,
                "sleep_min": sleep_min,
            }
        )
    return items
