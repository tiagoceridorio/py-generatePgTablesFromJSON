import json
from db_connection import get_db_connection
from table_operations import create_table, ensure_columns, insert_data

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
    with conn.cursor() as cursor:
        columns = ensure_columns(cursor, table_name, record)
        create_table(cursor, table_name, columns)
        insert_data(cursor, table_name, record)
    conn.commit()

def main():
    config_file = "config.json"
    conn = get_db_connection(config_file)

    file_path = "path_to_your_json_file.json"
    process_json_file(file_path, conn)
    conn.close()

if __name__ == "__main__":
    main()
