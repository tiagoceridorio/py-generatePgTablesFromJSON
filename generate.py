import json
import psycopg2
from psycopg2 import sql

def create_table(cursor, table_name, columns):
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
    cursor.execute(create_table_query)

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

def insert_data(cursor, table_name, json_obj):
    columns = json_obj.keys()
    values = [json.dumps(value) if isinstance(value, (dict, list)) else value for value in json_obj.values()]
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    cursor.execute(insert_query, values)

def process_json_file(file_path, conn):
    with open(file_path, 'r') as file:
        data = json.load(file)
        if isinstance(data, list):
            for record in data:
                process_json_record(record, conn)
        else:
            process_json_record(data, conn)

def process_json_record(record, conn):
    table_name = "orders"  # You can change this to a more appropriate table name
    columns = get_column_definitions(record)
    with conn.cursor() as cursor:
        create_table(cursor, table_name, columns)
        insert_data(cursor, table_name, record)
    conn.commit()

def load_db_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

def main():
    config_file = "config.json"
    db_config = load_db_config(config_file)
    
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )

    file_path = "path_to_your_json_file.json"
    process_json_file(file_path, conn)
    conn.close()

if __name__ == "__main__":
    main()
