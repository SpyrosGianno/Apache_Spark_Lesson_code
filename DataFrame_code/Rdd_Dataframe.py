from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_Dataframe").getOrCreate()

data = [("Leonidas",15,20),("Maria",18,12),("Thanasis",12,19)]
header =["Name","Grade","number"]

df = spark.createDataFrame(data,header)

df.show()
