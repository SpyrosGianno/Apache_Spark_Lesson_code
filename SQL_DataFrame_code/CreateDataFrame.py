from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("createDataFrame").getOrCreate()

row = ([(1258,"Eleni","Mathimatika", 7), (2258,"Nikos","Glossa",8), (3582,"Giannis","Mathimatik",5),
                  (4582,"Maria","Istoria",6),(6584,"Kostas","Glossa", 8), (7854,"Leonidas","Mathimatika", 7), 
                  (8898,"Marios","Mathimatika", 6), (9825,"Nikoleta","Glossa", 9),(8574,"Nikoleta","Mathimatika", 9)])

colums = ["id","name","Math","grade"]

stdGrade = spark.createDataFrame(row,colums)

stdGrade.show()
