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

def load_stock_data(symbol: str):
    df = spark.read \
    .option("header", True) \
    .csv("data/"+symbol+".csv")


    return df.select(
        df['Date'].cast(DateType()).alias('date'),
        df['Open'].cast(DoubleType()).alias('open'),
        df['Close'].cast(DoubleType()).alias('close'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low'),
    )

df = load_stock_data("AAPL")
df.show()
date_plus_2 = date_add(df['date'], 2)
date_as_string = date_plus_2.cast(StringType())
concat_column = concat(date_as_string,lit("Hello World"))

df.select(df['date'],concat_column.alias('transformedDate')) \
    .show(truncate=False)