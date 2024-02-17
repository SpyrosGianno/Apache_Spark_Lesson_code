from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_reduceByKey").getOrCreate()

data_person =(("Maria",1200),("Nikos",1500),("Giannis",1600),("Maria",1200),("Giannis",1600),("Panagiotis",1200))

persons = spark.sparkContext.parallelize(data_person)

for std in student_data_rdd.collect():
  print(std)

personRdmap = student_data_rdd.reduceByKey(lambda x,y:x+y).collect()

for student in personRdmap: 
  print(student)

