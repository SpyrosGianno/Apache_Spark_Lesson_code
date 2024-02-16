from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD_union").getOrCreate()

Rdper1 = "Makis Panou 1500, Nikos Argirou 2000, Nikoleta Panagiotarou 900 , Marilena Kapaki 1200, Dhmos Leonidou 2800, Nikos argirou 2000, Paniagiotis Loufas 1000 , Giannis Plakas 1200 , Nikoleta Panagiotarou 900 " .split(",")

Rdper2 =" Makis Panou 1500, Rea Marinou 3000, Nikoleta Panagiotarou 900 , Marilena Kapaki 1200, Dhmhtrhs Leo 2300, Nikos argirou 2100, Kostas fasarias 1100 , Rena Strati 1200 , Nikoleta Panagiotarou 900 ".split(",")

Rdper_Rdd1 = spark.sparkContext.parallelize(Rdper1)
Rdper_Rdd2 = spark.sparkContext.parallelize(Rdper2)

Rdper_Rdd1.collect()
Rdper_Rdd2.collect()

Rdd_union = Rdper_Rdd1.union(Rdper_Rdd2)
Rdper3 = Rdd_union.collect()

for n in Rdper3:
  print(n)
