from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("First_list").getOrCreate()

student_list ="maria,Nikos,Leonidas,Petros,Fatseas,Nikoleta,Markos".split(sep=",")
type(student_list)

student_list_RDD = spark.sparkContext.parallelize(student_list)

std_data=student_list_RDD.collect()

for word in std_data:
  print(word)

student_list_RDD.count()
