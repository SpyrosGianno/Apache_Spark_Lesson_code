from pyspark.sql import SparkSession
from pyspark.sql.functions import col as c

spark = SparkSession.builder.appName("filter").getOrCreate()

studf = spark.read.csv("studentGrade.csv",header = True )
studf.show()

studFilter = studf.filter(studf.grade == 10)
studFilter.show()
