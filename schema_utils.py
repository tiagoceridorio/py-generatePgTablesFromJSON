import json

def get_column_definitions(json_obj):
    columns = {}
    for key, value in json_obj.items():
        if isinstance(value, dict) and "$oid" in value:
            columns[key] = "VARCHAR"
        elif isinstance(value, dict) and "$date" in value:
            columns[key] = "TIMESTAMP"
        elif isinstance(value, bool):
            columns[key] = "BOOLEAN"
        elif isinstance(value, int):
            columns[key] = "INT"
        elif isinstance(value, float):
            columns[key] = "FLOAT"
        elif isinstance(value, str):
            columns[key] = "TEXT"
        elif isinstance(value, list):
            columns[key] = "JSONB"
        else:
            columns[key] = "JSONB"
    return columns
