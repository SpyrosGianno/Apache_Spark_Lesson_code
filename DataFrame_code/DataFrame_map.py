from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame_map()").getOrCreate()

data_student = [("Leonidas",15),("Maria",18),("Thanasis",13),("Nikos",17)]
header = ["Name","grades"]

df = spark.createDataFrame(data_student,header)
df.show()

def sum(vathmos): return (vathmos+2)

stdmap = df.rdd.map(lambda x: (x[0],sum(x[1]))).toDF(["Name","grades"])
stdmap.show()
