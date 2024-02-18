from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readCsv").getOrCreate()

df = spark.read.csv("csvFile.csv", header = True, inferSchema = False)

df.show()
