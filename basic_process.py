from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List, Any

from stdclasses import SchemaField


def basic_keyvalue(structured: List[SchemaField]) -> List[Dict]:
    keyValue: Dict = {}
    for item in structured:
        keyValue[item.name] = item.data
    return [keyValue]
