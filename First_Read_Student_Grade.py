from pyspark.sql import SparkSession,pandas as pd
url ="https://github.com/SpyrosGianno/Apache_Spark_Lesson_code/blob/0461dad0e6a3d8ef4b95d8f71ed8783a642841f5/Student_Grades_tst.csv"
spark = SparkSession.builder.appName("StudentGrades").getOrCreate()

df = spark.read.csv('C:\Student_Grades_tst.csv',header=True,inferSchema=True)

df.show()
