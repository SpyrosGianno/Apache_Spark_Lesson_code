from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("where_select_dis").getOrCreate()

personSallary = ([(125844, "Petros", "Iakovidis", 5000), (115485, "Giannis", "Leonidou",3000), (11585, "Maria", "Giannaki",1500), (138781, "Tasos", "Petropoulos",2000), (12595, "Nikoleta", "Panou",1800), (115485, "Giannis", "Leonidou",3000), (125844, "Petros", "Iakovidis", 5000), (185841, "Panagiota", "Manolou", 1500)])

colums = ["id", "firstname", "lastname", "salary"]

personSallaryDF = spark.createDataFrame(personSallary, colums)
personSallaryDF.show()
personSallaryDF.count()

whereSallary = personSallaryDF.where(personSallaryDF.salary < 2000)
whereSallary.show()

selectDis = personSallaryDF.select(personSallaryDF.id , personSallaryDF.firstname).distinct()
selectDis.show()

selectDis.count()
