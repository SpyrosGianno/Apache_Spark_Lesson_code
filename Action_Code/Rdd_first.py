from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_first()").getOrCreate()

data = [("Nikos", "25"), ("Makhs", "30"), ("Vagelis", "35")]

dfRdd = spark.sparkContext.parallelize(data)
dfRdd.collect()

dfRdd.first()
