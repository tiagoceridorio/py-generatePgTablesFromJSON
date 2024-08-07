import json
import logging
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection_string(config_file_path):
    try:
        with open(config_file_path, 'r') as file:
            config = json.load(file)
        connection_string = (
            f"dbname={config['dbname']} "
            f"user={config['user']} "
            f"password={config['password']} "
            f"host={config['host']} "
            f"port={config['port']}"
        )
        return connection_string
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        return None

def get_postgres_ids(connection_string, table_name):
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT _id FROM {table_name}")
        ids = cursor.fetchall()
        cursor.close()
        conn.close()
        return {str(id[0]) for id in ids}
    except Exception as e:
        logger.error(f"Error fetching IDs from PostgreSQL: {e}")
        return set()

def update_json_file(json_file_path, postgres_ids, updated_json_file_path):
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        updated_data = [record for record in data if record['_id']['$oid'] not in postgres_ids]

        with open(updated_json_file_path, 'w') as file:
            json.dump(updated_data, file, indent=4)

        logger.info(f"Updated JSON file saved to {updated_json_file_path}.")
    except Exception as e:
        logger.error(f"Error updating JSON file: {e}")

def main():
    config_file_path = "config.json"
    connection_string = get_postgres_connection_string(config_file_path)
    
    if connection_string is None:
        logger.error("No valid connection string could be created. Exiting.")
        return
    
    table_name = "orders"
    json_file_path = "path_to_your_json_file.json"
    updated_json_file_path = "path_to_updated_json_file.json"

    postgres_ids = get_postgres_ids(connection_string, table_name)
    logger.info(f"Fetched {len(postgres_ids)} IDs from PostgreSQL.")

    update_json_file(json_file_path, postgres_ids, updated_json_file_path)

if __name__ == "__main__":
    main()
