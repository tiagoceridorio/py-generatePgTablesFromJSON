import json
from psycopg2 import sql
from schema_utils import get_column_definitions

def create_table(cursor, table_name, columns):
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
    cursor.execute(create_table_query)

def add_columns(cursor, table_name, columns):
    for col, dtype in columns.items():
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {dtype};"
        cursor.execute(alter_table_query)

def ensure_columns(cursor, table_name, json_obj):
    columns = get_column_definitions(json_obj)
    add_columns(cursor, table_name, columns)
    return columns

def insert_data(cursor, table_name, json_obj):
    columns = json_obj.keys()
    values = [json.dumps(value) if isinstance(value, (dict, list)) else value for value in json_obj.values()]
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    cursor.execute(insert_query, values)
