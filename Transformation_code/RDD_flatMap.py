from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD_flatMap").getOrCreate()

students_data ="Giorgos A ,Elpida A,Giannis c,Aggelos A,Marija B,Anna A".split(",")

students_rdd = spark.sparkContext.parallelize(students_data)
students_rdd.collect()

#Efarmozoume thn sinartish flatMap()
students = students_rdd.flatMap(lambda x: x.split(" "))
students.collect()
# mas epistrefei to arithmo twn gramatwn 
students.count()
students_rdd.count()
