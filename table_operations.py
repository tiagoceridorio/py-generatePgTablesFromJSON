import json
from psycopg2 import sql
from schema_utils import get_column_definitions, process_value
import logging
from table_initialization import create_table  # Importando a função create_table

def add_columns(cursor, table_name, columns):
    for col, dtype in columns.items():
        if '$' not in col:
            alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {dtype};"
            logging.info(f"Altering table with query: {alter_table_query}")
            cursor.execute(alter_table_query)

def ensure_columns(cursor, table_name, json_obj):
    columns = get_column_definitions(json_obj)
    add_columns(cursor, table_name, columns)
    return columns

def insert_data(cursor, table_name, json_obj):
    columns = [key.lower() for key in json_obj.keys() if '$' not in key]
    values = [process_value(value) for key, value in json_obj.items() if '$' not in key]
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    logging.info(f"Inserting data with query: {insert_query.as_string(cursor)}")
    cursor.execute(insert_query, values)

    for key, value in json_obj.items():
        if isinstance(value, dict) and '$' not in key:
            nested_table_name = f"{table_name}_{key}"
            create_and_insert_nested_data(cursor, nested_table_name, value, json_obj.get('_id'))
        elif isinstance(value, list):
            for item in value:
                nested_table_name = f"{table_name}_{key}"
                create_and_insert_nested_data(cursor, nested_table_name, item, json_obj.get('_id'))

def create_and_insert_nested_data(cursor, table_name, json_obj, parent_id):
    if not isinstance(json_obj, dict):
        return
    
    json_obj['_parent_id'] = parent_id['$oid'] if parent_id and '$oid' in parent_id else parent_id
    columns = get_column_definitions(json_obj)
    create_table(cursor, table_name, columns)  # Garantindo que a tabela seja criada antes da inserção
    insert_data(cursor, table_name, json_obj)
