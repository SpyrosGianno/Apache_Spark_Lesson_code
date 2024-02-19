from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("distinct").getOrCreate()

studG = spark.read.csv("studGrade.csv", header=True)
studG.show()

disStud = studG.distinct()
disStud.show()
