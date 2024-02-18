from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("count_look").getOrCreate()

student_grade = ([("Eleni", 7), ("Nikos", 8), ("Giannis", 3), ("Maria", 4),("Kostas", 8), ("Eleni", 7), ("Marios", 6), ("Nikoleta", 9) ("Nikoleta", 9)])

stdGr = spark.sparkContext.parallelize(student_grade)

for stf in stdGr.collect():
  print(stf)

studCountBy = stdGr.countByKey()
studLoc = stdGr.lookup("Eleni")

print(studCountBy)

print(studLoc)
