from pyspark.sql import SparkSession
from pyspark.sql.functions import col as c

spark = SparkSession.builder.appName("Sort").getOrCreate()

studf = spark.read.csv("studentGrade.csv",header = True )
studf.show()

studShow = studf.sort("grade")
stdShow.show()
