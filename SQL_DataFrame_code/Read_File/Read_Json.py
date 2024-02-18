from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readJson").getOrCreate()

df = spark.read.json("jsonFile.json", header=True, inferSchema=True")
df.show()
