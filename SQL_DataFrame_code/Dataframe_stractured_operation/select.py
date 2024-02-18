#xrhsh ths select()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sellect").getOrCreate()

students = spark.read.csv("studentGrade.csv",header = True )
students.show()

nameID = students.select("id", "name")
nameID.show()
