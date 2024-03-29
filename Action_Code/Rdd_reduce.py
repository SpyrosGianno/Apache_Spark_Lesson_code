#ypologismos apodixewn
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_reduce").getOrCreate()

data = ([1500,2000,150,80,1800,3500,4200,1900,3000,1500,900,750])

rdCost = spark.sparkContext.parallelize(data)
rdCost.collect()

#pragmatopoiei athrisma twn stixeiwn tou RDD
yearKerd = rdCost.reduce(lambda poso1,poso2: poso1+poso2)
print(yearKerd)

#ypologizei to sinoliko poso twn apodixewn pou tha prepei na mazepsei
Apodixeis = yearKerd*10/100
print(Apodixeis)
