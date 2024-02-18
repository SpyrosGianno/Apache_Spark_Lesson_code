#read txt file
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readTxt").getOrCreate()

df = spark.read.text("txtfile.txt")
df.show()
