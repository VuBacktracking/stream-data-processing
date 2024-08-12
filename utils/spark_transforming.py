import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, round
import os
from dotenv import load_dotenv

load_dotenv()

SPARK_HOME = os.getenv('SPARK_HOME')
findspark.init(SPARK_HOME)
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Products Transformation") \
    .master("local[*]") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.format('csv').load("data/products.csv", header=True, inferSchema=True)

# Drop unnecessary columns
df = df.drop("Unnamed: 0", "description", "pay_later", "has_video", "date_created", "vnd_cashback")

# Trim the unnecessary spaces in fulfillment_type
df = df.withColumn("fulfillment_type", trim(col("fulfillment_type")))

# Create a new column 'discount' based on original_price and price
df = df.withColumn("discount", 
                   round(((col("original_price") - col("price")) / col("original_price")) * 100, 2))

# Save the transformed DataFrame to the specified directory
df.coalesce(1).write.csv("data/out/products.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()