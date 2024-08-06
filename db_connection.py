import json
import psycopg2

def load_db_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

def get_db_connection(config_file):
    db_config = load_db_config(config_file)
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    return conn
