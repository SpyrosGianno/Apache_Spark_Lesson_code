#xrhsh ths select()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("where").getOrCreate()

students = spark.read.csv("studentGrade.csv",header = True )
students.show()

where_gRrantStud = students.where(students.grade == 10)
where_gRrantStud.show()
