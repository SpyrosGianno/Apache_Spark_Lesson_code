from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame_union").getOrCreate()

Cost_Person1 = [(1,"Manos","Panagiotou",1800),(2,"Elpida","Maraki",1200),(3,"Giannis","stefanou",1000,),(4,"Maria","Polidei",2508),(5,"Agelos","Spirou",1177),(6,"Katerina","Theoxari",1900),(7,"Dhmhtra","Tsopou",1999)]

Cost_Person2 = [(1,"Kostas","Panagiotou",800),(2,"Elpida","Eteokli",1000),(3,"Giannis","stefanou",1000,),(4,"Maria","Polipeyki",1151),(5,"Arhs","Spirelos",1117),(6,"Katerina","Theoxari",1900),(7,"Dhmhtra","Tsopou",1999)]

colum = ["id","name","surname","salary"]

dfCost1 = spark.createDataFrame(Cost_Person1,colum)
dfCost2 = spark.createDataFrame(Cost_Person2,colum)

dfCost1.show()
dfCost2.show()

dfcost3 = dfCost1.union(dfCost2)
dfdist3 = dfcost3.distinct()

dfdist3.show()
