from pyspark.sql import SparkSession,pandas as pd
url =
spark = SparkSession.builder.appName("StudentGrades").getOrCreate()

df = spark.read.csv('C:\Student_Grades_tst.csv',header=True,inferSchema=True)

df.show()
