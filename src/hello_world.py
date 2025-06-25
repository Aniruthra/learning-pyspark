import os



import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Create Spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Define schema
schema = StructType([
    StructField("seq", StringType(), True)
])

# Input data
dates = [('1',)]  # Correct: List of tuples, each tuple is a row

# Create DataFrame
df = spark.createDataFrame(dates, schema=schema)

# Show DataFrame
df.show()
