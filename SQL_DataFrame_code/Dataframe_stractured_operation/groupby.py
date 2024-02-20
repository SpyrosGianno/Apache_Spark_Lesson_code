from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("limit").getOrCreate()

dfRead = spark.read.csv("sallary.csv", header=True)
dfRead.show()


dfGrp = dfRead.groupBy(dfRead.columns[3])
dfGrp.show
