import json
from db_connection import get_db_connection
from table_operations import ensure_table_and_columns, insert_data
import logging

# Configuração do logging para exibir apenas mensagens WARNING ou superiores
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

def process_json_file(file_path, conn):
    with open(file_path, 'r') as file:
        data = json.load(file)
        if isinstance(data, list):
            for record in data:
                try:
                    process_json_record(record, conn)
                except Exception as e:
                    logging.error(f"Error processing record: {e}")
                    break
        else:
            try:
                process_json_record(data, conn)
            except Exception as e:
                logging.error(f"Error processing record: {e}")

def process_json_record(record, conn):
    table_name = "orders"
    with conn.cursor() as cursor:
        ensure_table_and_columns(cursor, table_name, record)
        insert_data(cursor, table_name, record)
    conn.commit()

def main():
    config_file = "config.json"
    conn = get_db_connection(config_file)

    file_path = "path_to_updated_json_file.json"
    
    process_json_file(file_path, conn)
    
    conn.close()

if __name__ == "__main__":
    main()
