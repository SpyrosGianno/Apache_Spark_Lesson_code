from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Where & show").getOrCreate()

studf = spark.read.csv("studentGrade.csv",header = True )
studf.show()

studentWher = studf.where(studf.grade <= 7)
studentSort = studentWher.sort(studentWher.id)

studentSort.show()
