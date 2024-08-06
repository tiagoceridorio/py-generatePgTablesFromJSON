import logging
from psycopg2 import sql

def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE IF NOT EXISTS {table_name} CASCADE;"
    logging.info(f"Dropping table with query: {drop_table_query}")
    cursor.execute(drop_table_query)

def create_table(cursor, table_name, columns):
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
    logging.info(f"Creating table with query: {create_table_query}")
    cursor.execute(create_table_query)

def initialize_table(cursor, table_name):
    columns = {
        "_id": "VARCHAR",
        "cartid": "VARCHAR",
        "ismobile": "BOOLEAN",
        "ip": "TEXT",
        "store": "JSONB",
        "customer": "JSONB",
        "sellers": "JSONB",
        "items": "JSONB",
        "volumes": "JSONB",
        "payment": "JSONB",
        "refundrules": "JSONB",
        "stateregistration": "TEXT",
        "createdinadmin": "BOOLEAN",
        "adminid": "TEXT",
        "ordersiteid": "TEXT",
        "date": "TIMESTAMP",
        "documenttype": "TEXT",
        "iscrisis": "BOOLEAN",
        "iscrisismarketing": "BOOLEAN",
        "isinternational": "BOOLEAN",
        "itemscount": "INT",
        "ordersource": "TEXT",
        "searchkey": "TEXT",
        "status": "TEXT",
        "values": "JSONB",
        "cubbo": "JSONB",
        "history": "JSONB"
    }
    drop_table(cursor, table_name)
    create_table(cursor, table_name, columns)
