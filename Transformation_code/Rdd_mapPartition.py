from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_mapPartition").getOrCreate()

number_data =[2,10,8,9,12,18,25,32,5]

numSystem = spark.sparkContext.parallelize(number_data,4)
  numSystem.collect()

def num_partition(numprt): 
  yield sum(numprt)

numrdd = numSystem.mapPartitions(num_partition)
numrdd.collect()

def num_partitions(nums,numprt): 
  yield nums

numrdds = numSystem.mapPartitionsWithIndex(num_partitions)
numrdds.collect()
