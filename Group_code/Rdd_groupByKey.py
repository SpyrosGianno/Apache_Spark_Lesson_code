from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_groupByKey").getOrCreate()

student_data =(("Maria",12),("Nikos",15),("Giannis",16),("Maria",12))

student_data_rdd = spark.sparkContext.parallelize(student_data)

#klish ths synartishs groupByKeyollect()
studentDmap = student_data_rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()


for student in studentDmap: 
  print(student)
