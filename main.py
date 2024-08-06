import json
from db_connection import get_db_connection
from table_operations import ensure_columns, insert_data, initialize_table
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_json_file(file_path, conn):
    with open(file_path, 'r') as file:
        data = json.load(file)
        if isinstance(data, list):
            for record in data:
                process_json_record(record, conn)
        else:
            process_json_record(data, conn)

def process_json_record(record, conn):
    table_name = "orders"  # Você pode mudar isso para um nome de tabela mais apropriado
    with conn.cursor() as cursor:
        ensure_columns(cursor, table_name, record)  # Certifique-se de que todas as colunas necessárias são criadas
        insert_data(cursor, table_name, record)
    conn.commit()

def main():
    config_file = "config.json"
    conn = get_db_connection(config_file)

    # Inicializa a tabela uma vez ao rodar o script
    with conn.cursor() as cursor:
        initialize_table(cursor, "orders")
    conn.commit()

    file_path = "path_to_your_json_file.json"
    process_json_file(file_path, conn)
    conn.close()

if __name__ == "__main__":
    main()
