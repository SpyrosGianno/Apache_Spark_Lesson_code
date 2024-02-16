from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_map()").getOrCreate()

number_data =[2,10,8,9,12,18,25,32,5]

rdd_number = spark.sparkContext.parallelize(number_data)
rdd_number.collect()

new_number = rdd_number.map(lambda x: x*x)
new_number.collect()
