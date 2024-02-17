from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_take()").getOrCreate()

data = "Nikos,Manos,Makhs,Vagelis,Stelios,Stathis, Mitsos,Kostas".split(",")

dftake = spark.sparkContext.parallelize(data)
dftake.collect()

dftake.take(5)
