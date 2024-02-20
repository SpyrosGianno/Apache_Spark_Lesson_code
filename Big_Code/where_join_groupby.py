from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("serch_where_join_groupby").getOrCreate()

personSallary = ([(125844, "Petros", "Iakovidis", 5000), (115485, "Giannis", "Leonidou",3000), (11585, "Maria", "Giannaki",1500), (138781, "Tasos", "Petropoulos",2000), (12595, "Nikoleta", "Panou",1800), (115485, "Giannis", "Leonidou",3000), (125844, "Petros", "Iakovidis", 5000), (185841, "Panagiota", "Manolou", 1500)])

job =([(125844,"CEO"),(115485,"Manager"),(11585,"IT"),(138781,"Desygner"),(12595,"Dev-ops")])

colums = ["id", "firstname", "lastname", "salary"]
colum2 = ["id","job"]

dfRead = spark.createDataFrame(personSallary, colums)
dfRead.show()

#anazhthsh onomatos
SearchPerson = dfRead.where(dfRead.lastname == "Leonidou")
SearchPerson.show()

# anazhthsh kai athrisma
wherePers = dfRead.where(dfRead.lastname == "Iakovidis").groupby(dfRead.columns[3]).sum(dfRead.columns[3])
wherePers.show()

#dhmiourgia neou dataframe
jobRead = spark.createDataFrame(job, colum2)
jobRead.show()

dfJoin = dfRead.join(jobRead, dfRead.id == jobRead.id)
dfJoin.show()

