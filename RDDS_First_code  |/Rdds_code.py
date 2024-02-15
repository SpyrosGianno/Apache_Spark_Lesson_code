from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD_FirstCreate").getOrCreate()

df = spark.sparkContext.parallelize([("Makis",8,10),("Nikos",9,10),("Eleni",7,20),("Maria",10,6)]).toDF(["Name","Grade1","Grade2"])

df.show()
