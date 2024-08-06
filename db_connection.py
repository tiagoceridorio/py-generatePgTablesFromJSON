import json
import psycopg2
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_db_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

def get_db_connection(config_file):
    db_config = load_db_config(config_file)
    try:
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
            options='-c client_encoding=UTF8'
        )
        logging.info("Conexão com o banco de dados PostgreSQL estabelecida com sucesso.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Falha ao conectar no banco de dados PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    # Teste de conexão
    config_file = "config.json"
    try:
        connection = get_db_connection(config_file)
        connection.close()
    except Exception as e:
        logging.error("Erro durante o teste de conexão.")
