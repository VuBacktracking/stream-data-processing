import os
import logging
import pandas as pd
from time import sleep
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

# Configure logging
logging.basicConfig(
    filename='logs/insert_and_stream.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB = os.getenv('POSTGRES_DB')
USER= os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')

TABLE_NAME = "products"
DATA_FILE = './data/products.csv'
NUM_ROWS = 2000

def insert_table(data_file, table_name, num_rows):
    pc = PostgresSQLClient(
        database=DB,
        user=USER,
        password=PASSWORD
    )
    
    columns = []  # Initialize columns to avoid UnboundLocalError
    try:
        columns = pc.get_columns(table_name=table_name)
        if not columns:
            raise ValueError(f"No columns found for table {table_name}")
        logging.info(f"Columns for table '{table_name}': {columns}")
    except Exception as e:
        logging.error(f"Failed to get schema for table with error: {e}")
        return

    try:
        df = pd.read_csv(data_file, nrows=num_rows)
        logging.info(f"Read {num_rows} rows from CSV file '{data_file}'.")
    except Exception as e:
        logging.error(f"Failed to read CSV file with error: {e}")
        return

    for _, row in df.iterrows():
        values = ",".join(map(lambda v: f"'{v}'" if isinstance(v, str) else str(v), row.values))
        query = f"""
                    INSERT INTO {table_name} ({",".join(columns)})
                    VALUES ({values})
                """
        logging.info(f"Executing query: {query}")  # Log the query
        try:
            pc.execute_query(query)
            logging.info("Query executed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute query with error: {e}")
        sleep(4)

if __name__ == "__main__":
    insert_table(DATA_FILE, TABLE_NAME, NUM_ROWS)
