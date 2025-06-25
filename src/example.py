import os
os.environ['JAVA_HOME'] = r'C:\Users\welcom\.jdks\corretto-1.8.0_452'


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("load-csv") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv("data/AAPL.csv")

df2=df.select("Date","Open").limit(10)
joined = df.join(df2,"Date")
joined.show()

