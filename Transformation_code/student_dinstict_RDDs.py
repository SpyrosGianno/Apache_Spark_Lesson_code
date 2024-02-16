from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("student_dinstict_RDDs").getOrCreate()

students_data ="Giorgos,Elpida,Giannis,Aggelos,Marija,Anna,Giorgos".split(",")

student_Rdds = spark.sparkContext.parallelize(students_data)
student_Rdds.collect()

student_classis = student_Rdds.distinct()
student_classis.collect()
