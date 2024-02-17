from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_reduceByKey").getOrCreate()

student_data =(("Maria",1200),("Nikos",1500),("Giannis",1600),("Maria",1200),("Giannis",1600),("Panagiotis",1200))

student_data_rdd = spark.sparkContext.parallelize(student_data)

for std in student_data_rdd.collect():
  print(std)

studentDmap = student_data_rdd.reduceByKey(lambda x,y:x+y).collect()

for student in studentDmap: 
  print(student)
