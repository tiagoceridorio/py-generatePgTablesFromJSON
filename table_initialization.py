import logging
from psycopg2 import sql
from schema_utils import get_column_definitions

def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE IF EXISTS {table_name.lower()} CASCADE;"
    logging.info(f"Dropping table with query: {drop_table_query}")
    cursor.execute(drop_table_query)

def create_table(cursor, table_name, columns):
    column_defs = ", ".join([f"{col.lower()} {dtype}" for col, dtype in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name.lower()} ({column_defs});"
    logging.info(f"Creating table with query: {create_table_query}")
    cursor.execute(create_table_query)

def get_nested_table_definitions(base_table, json_obj):
    nested_tables = {}
    for key, value in json_obj.items():
        if isinstance(value, dict) and '$' not in key and key != "_id":
            nested_table_name = f"{base_table.lower()}_{key.lower()}"
            nested_tables[nested_table_name] = get_column_definitions(value)
            nested_tables.update(get_nested_table_definitions(nested_table_name, value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested_table_name = f"{base_table.lower()}_{key.lower()}"
                    nested_tables[nested_table_name] = get_column_definitions(item)
                    nested_tables.update(get_nested_table_definitions(nested_table_name, item))
    return nested_tables

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
    columns['_parent_id'] = "VARCHAR"
    logging.info(f"Generated columns: {columns}")
    return columns

def initialize_table(cursor, base_table, json_obj):
    drop_table(cursor, base_table)
    columns = get_column_definitions(json_obj)
    create_table(cursor, base_table, columns)

    nested_tables = get_nested_table_definitions(base_table, json_obj)
    for nested_table, nested_columns in nested_tables.items():
        create_table(cursor, nested_table, nested_columns)
