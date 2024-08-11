"""
This script gets the streaming data from Kafka topic, then writes it to MinIO
"""

import sys
import warnings
import traceback
import logging
from pyspark import SparkConf, SparkContext
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/minio_streaming/products/checkpoints"

def create_spark_session():
    """
    Creates the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    try:
        builder = SparkSession.builder \
                    .appName("Streaming Kafka") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()

        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def read_minio_credentials():
    import os
    from dotenv import load_dotenv

    load_dotenv()
    try:
        accessKeyId = os.getenv('MINIO_ACCESS_KEY')
        secretAccessKey = os.getenv('MINIO_SECRET_KEY')
        endpoint = os.getenv('MINIO_ENDPOINT')
        logging.info('MinIO credentials obtained correctly')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO credentials couldn't be obtained due to exception: {e}")

    return accessKeyId, secretAccessKey, endpoint


def load_minio_config(spark_context: SparkContext):
    accessKeyId, secretAccessKey, endpoint = read_minio_credentials()
    
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.warning(f"MinIO config could not be created successfully due to exception: {e}")


def create_initial_dataframe(spark_session):
    try: 
        df = (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "sales.public.products")
            .option("failOnDataLoss", "false")
            .load())
        logging.info("Initial dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Initial dataframe could not be created due to exception: {e}")

    return df

def decode_base64_to_decimal(base64_str):
    """
    Decode base64 to decimal
    """
    import base64
    from decimal import Decimal

    if base64_str:
        # Fix padding
        missing_padding = len(base64_str) % 4
        if missing_padding:
            base64_str += '=' * (4 - missing_padding)
        decoded_bytes = base64.b64decode(base64_str)
        # Convert bytes to integer and then to decimal with scale 2
        return Decimal(int.from_bytes(decoded_bytes, byteorder='big')) / Decimal(100)
    return None

def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe
    """
    from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField, DecimalType
    from pyspark.sql.functions import col, from_json, udf

    payload_after_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", StringType(), True),
            StructField("fulfillment_type", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("rating_average", StringType(), True),
            StructField("current_seller", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity_sold", IntegerType(), True)
])

    schema = StructType([
        StructField("payload", StructType([
            StructField("after", payload_after_schema, True)
        ]), True)
    ])

    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), schema).alias("data")) \
                .select("data.payload.after.*")

    decode_base64_to_decimal_udf = udf(decode_base64_to_decimal, DecimalType(12, 2))

    decoded_df = parsed_df \
            .withColumn("price", decode_base64_to_decimal_udf(col("price"))) \
            .withColumn("rating_average", decode_base64_to_decimal_udf(col("rating_average"))) \

    decoded_df.createOrReplaceTempView("products_view")

    df_final = spark.sql("""
        SELECT 
            *
        FROM products_view 
    """)

    logging.info("Final dataframe created successfully!")
    return df_final


def start_streaming(df):
    minio_bucket = 'datalake'

    logging.info("Streaming is being started...")
    stream_query = df.writeStream \
                        .format("delta") \
                        .outputMode("append") \
                        .option("checkpointLocation", checkpointDir) \
                        .option("path", f"s3a://{minio_bucket}/products") \
                        .partitionBy("brand") \
                        .start()

    return stream_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df = create_initial_dataframe(spark)
    if df.isStreaming:
        df_final = create_final_dataframe(df, spark)
        start_streaming(df_final)
    else:
        logging.info("Kafka messages is stopped.")