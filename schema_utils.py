import json
import logging

def get_column_definitions(json_obj):
    columns = {}
    for key, value in json_obj.items():
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
        elif isinstance(value, list):
            columns[column_name] = "JSONB"
        else:
            columns[column_name] = "JSONB"
    logging.info(f"Generated columns: {columns}")
    return columns
