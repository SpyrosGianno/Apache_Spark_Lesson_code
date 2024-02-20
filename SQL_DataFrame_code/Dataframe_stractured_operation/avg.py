from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StudentGrades").getOrCreate()

df = spark.read.csv("Student_Grades_tst.csv",header=True,inferSchema=True)
df.show()

salAvg = df.agg({df.columns[4]: "avg"})
salAvg.show()
