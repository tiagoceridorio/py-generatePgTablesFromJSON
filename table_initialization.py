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
        if isinstance(value, dict) and '$' not in key and key not in ["_id", "cartId"]:
            nested_table_name = f"{base_table.lower()}_{key.lower()}"
            nested_tables[nested_table_name] = get_column_definitions(value)
            nested_tables[nested_table_name]['_parent_id'] = "VARCHAR"  # Adiciona a coluna _parent_id para todas as tabelas aninhadas
            nested_tables.update(get_nested_table_definitions(nested_table_name, value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested_table_name = f"{base_table.lower()}_{key.lower()}"
                    nested_tables[nested_table_name] = get_column_definitions(item)
                    nested_tables[nested_table_name]['_parent_id'] = "VARCHAR"  # Adiciona a coluna _parent_id para todas as tabelas aninhadas
                    nested_tables.update(get_nested_table_definitions(nested_table_name, item))
    return nested_tables

def initialize_table(cursor, base_table, json_obj):
    drop_table(cursor, base_table)
    columns = get_column_definitions(json_obj)
    create_table(cursor, base_table, columns)

    nested_tables = get_nested_table_definitions(base_table, json_obj)
    for nested_table, nested_columns in nested_tables.items():
        create_table(cursor, nested_table, nested_columns)
