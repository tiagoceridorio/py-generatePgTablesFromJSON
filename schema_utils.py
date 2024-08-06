import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_column_definitions(json_obj):
    columns = {}
    for key, value in json_obj.items():
        if '$' in key:
            continue
        column_name = key.lower()
        if isinstance(value, dict) and "$oid" in value:
            columns[column_name] = "VARCHAR"
        elif isinstance(value, dict) and "$date" in value:
            columns[column_name] = "TIMESTAMP"
        elif isinstance(value, bool):
            columns[column_name] = "BOOLEAN"
        elif isinstance(value, int):
            columns[column_name] = "INT"
        elif isinstance(value, float):
            columns[column_name] = "FLOAT"
        elif isinstance(value, str):
            columns[column_name] = "TEXT"
        elif isinstance(value, list) or isinstance(value, dict):
            columns[column_name] = "JSONB"
        else:
            columns[column_name] = "JSONB"
    logger.info(f"Generated columns: {columns}")
    return columns

def process_value(value):
    if isinstance(value, dict) and "$date" in value:
        return datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
    elif isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value)
    return value
