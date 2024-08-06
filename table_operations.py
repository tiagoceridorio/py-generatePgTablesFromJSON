import json
from psycopg2 import sql
from schema_utils import get_column_definitions, process_value
import logging

def add_columns(cursor, table_name, columns):
    for col, dtype in columns.items():
        if '$' not in col:
            alter_table_query = f"ALTER TABLE {table_name.lower()} ADD COLUMN IF NOT EXISTS {col.lower()} {dtype};"
            logging.info(f"Altering table with query: {alter_table_query}")
            cursor.execute(alter_table_query)

def ensure_table_and_columns(cursor, table_name, json_obj):
    columns = get_column_definitions(json_obj)
    create_table_if_not_exists(cursor, table_name, columns)
    add_columns(cursor, table_name, columns)
    return columns

def create_table_if_not_exists(cursor, table_name, columns):
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name.lower()} ({', '.join([f'{col.lower()} {dtype}' for col, dtype in columns.items()])});"
    logging.info(f"Creating table with query: {create_table_query}")
    cursor.execute(create_table_query)

def ensure_columns(cursor, table_name, json_obj):
    columns = get_column_definitions(json_obj)
    add_columns(cursor, table_name, columns)

def insert_data(cursor, table_name, json_obj):
    ensure_columns(cursor, table_name, json_obj)
    columns = [key.lower() for key in json_obj.keys() if '$' not in key and key not in ["_id", "cartId"] and not isinstance(json_obj[key], (dict, list))]
    values = [process_value(value) for key, value in json_obj.items() if '$' not in key and key not in ["_id", "cartId"] and not isinstance(json_obj[key], (dict, list))]
    
    if '_id' in json_obj:
        columns.append('_id')
        values.append(json_obj['_id']['$oid'])
    if 'cartId' in json_obj:
        columns.append('cartid')
        values.append(json_obj['cartId']['$oid'])
    
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name.lower()),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    logging.info(f"Inserting data with query: {insert_query.as_string(cursor)}")
    cursor.execute(insert_query, values)

    for key, value in json_obj.items():
        if isinstance(value, dict) and '$' not in key and key not in ["_id", "cartId"]:
            nested_table_name = f"{table_name.lower()}_{key.lower()}"
            logging.info(f"Ensuring table and columns for nested table: {nested_table_name}")
            ensure_table_and_columns(cursor, nested_table_name, value)
            insert_nested_data(cursor, nested_table_name, value, json_obj.get('_id'))
        elif isinstance(value, list):
            for item in value:
                nested_table_name = f"{table_name.lower()}_{key.lower()}"
                logging.info(f"Ensuring table and columns for nested table: {nested_table_name}")
                ensure_table_and_columns(cursor, nested_table_name, item)
                insert_nested_data(cursor, nested_table_name, item, json_obj.get('_id'))

def insert_nested_data(cursor, table_name, json_obj, parent_id):
    if not isinstance(json_obj, dict):
        return

    json_obj['_parent_id'] = parent_id['$oid'] if parent_id and '$oid' in parent_id else parent_id
    insert_data(cursor, table_name, json_obj)
