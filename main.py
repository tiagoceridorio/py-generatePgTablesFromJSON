import json
from db_connection import get_db_connection
from table_operations import ensure_table_and_columns, insert_data
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_tables_for_all_records(file_path, conn):
    with open(file_path, 'r') as file:
        data = json.load(file)
        if isinstance(data, list):
            for record in data:
                create_tables_for_record(record, conn)
        else:
            create_tables_for_record(data, conn)

def create_tables_for_record(record, conn):
    table_name = "orders"
    with conn.cursor() as cursor:
        ensure_table_and_columns(cursor, table_name, record)
    conn.commit()

def insert_data_for_all_records(file_path, conn):
    with open(file_path, 'r') as file:
        data = json.load(file)
        if isinstance(data, list):
            for record in data:
                insert_data_for_record(record, conn)
        else:
            insert_data_for_record(data, conn)

def insert_data_for_record(record, conn):
    table_name = "orders"
    with conn.cursor() as cursor:
        insert_data(cursor, table_name, record)
    conn.commit()

def main():
    config_file = "config.json"
    conn = get_db_connection(config_file)

    file_path = "path_to_your_json_file.json"
    
    # Primeiro passo: criar tabelas
    create_tables_for_all_records(file_path, conn)
    
    # Segundo passo: inserir dados
    insert_data_for_all_records(file_path, conn)
    
    conn.close()

if __name__ == "__main__":
    main()
