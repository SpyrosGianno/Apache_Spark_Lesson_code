from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("orderby").getOrCreate()

studf = spark.read.csv("studentGrade.csv", header=True)
studf.show()

studentOrder = studf.orderBy(studf.id , ascending=True)
studentOrder.show()
