import json
from psycopg2 import sql
from schema_utils import get_column_definitions, process_value
import logging

def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    logging.info(f"Dropping table with query: {drop_table_query}")
    cursor.execute(drop_table_query)

def create_table(cursor, table_name, columns):
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
    logging.info(f"Creating table with query: {create_table_query}")
    cursor.execute(create_table_query)

def add_columns(cursor, table_name, columns):
    for col, dtype in columns.items():
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {dtype};"
        logging.info(f"Altering table with query: {alter_table_query}")
        cursor.execute(alter_table_query)

def ensure_columns(cursor, table_name, json_obj):
    columns = get_column_definitions(json_obj)
    drop_table(cursor, table_name)  # Drop the table if it exists
    create_table(cursor, table_name, columns)  # Certifique-se de que a tabela é criada antes de adicionar colunas
    add_columns(cursor, table_name, columns)
    return columns

def insert_data(cursor, table_name, json_obj):
    columns = [key.lower() for key in json_obj.keys()]
    values = [process_value(value) for value in json_obj.values()]
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    logging.info(f"Inserting data with query: {insert_query.as_string(cursor)}")
    cursor.execute(insert_query, values)
