from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("drop").getOrCreate()

studf = spark.read.csv("studentGrade.csv",header = True )
studf.show()

#klish drop 
studentWher = studf.drop(studf.columns[0]) 
studentWher.show()
