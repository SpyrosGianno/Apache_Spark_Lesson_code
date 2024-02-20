from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("groupby").getOrCreate()

personSallary = ([(125844, "Petros", "Iakovidis", 5000), (115485, "Giannis", "Leonidou",3000), (11585, "Maria", "Giannaki",1500), (138781, "Tasos", "Petropoulos",2000), (12595, "Nikoleta", "Panou",1800), (115485, "Giannis", "Leonidou",3000), (125844, "Petros", "Iakovidis", 5000), (185841, "Panagiota", "Manolou", 1500)])

colums = ["id", "firstname", "lastname", "salary"]

dfRead = spark.createDataFrame(personSallary, colums)
dfRead.show()


dfGrp = dfRead.groupBy(dfRead.columns[3]).sum(dfRead.columns[3])
dfGrp.show()
