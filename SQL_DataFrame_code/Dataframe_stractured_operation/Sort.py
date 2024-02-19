from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sort").getOrCreate()

studf = spark.read.csv("studentGrade.csv",header = True )
studf.show()

studShow = studf.sort("grade")
stdShow.show()
