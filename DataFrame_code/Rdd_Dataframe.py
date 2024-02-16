from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_Dataframe").getOrCreate()

df = spark.sparkContext.createDataFrame([("Leonidas",15,20),("Maria",18,12),("Thanasis",12,19)]).toDF(["Name","Grade","number"])

df.show()
