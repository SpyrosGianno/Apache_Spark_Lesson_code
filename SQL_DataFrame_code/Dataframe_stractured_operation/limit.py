from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("limit").getOrCreate()

dfRead = spark.read.csv("studGrade.csv", header=True)
dfRead.show()

dfLim = dfRead.limit(3)
dfLim.show()
