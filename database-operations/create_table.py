import os
import logging
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

# Configure logging
logging.basicConfig(
    filename='log/create_table.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

DB = os.getenv('POSTGRES_DB')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')

def create_products_table():
    pc = PostgresSQLClient(
        database=DB,
        user=USER,
        password=PASSWORD
    )

    create_products_table_query = """
        DROP TABLE IF EXISTS products;
        CREATE TABLE products (
            id                  VARCHAR(50),           
            name                VARCHAR(255),
            description         TEXT,
            original_price      NUMERIC(10, 2),
            price               NUMERIC(10, 2),
            fulfillment_type    VARCHAR(50),
            brand               VARCHAR(100),
            review_count        INTEGER,
            rating_average      NUMERIC(3, 2),
            favourite_count     INTEGER,
            current_seller      VARCHAR(255),
            number_of_images    INTEGER,
            has_video           BOOLEAN,
            category            VARCHAR(100),
            quantity_sold       INTEGER,
            discount            NUMERIC(5, 2) 
        );
    """

    try: 
        pc.execute_query(create_products_table_query)
        logging.info("Table 'products' created successfully.")
    except Exception as e:
        logging.error(f"Failed to create table with error: {e}")

if __name__ == "__main__":
    create_products_table()
