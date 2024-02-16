from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_Dataframe_Filter").getOrCreate()

Students_data = [(1,"Giorgos",8,"A"),(2,"Elpida",6,"A"),(3,"Giannis",8,"C"),(4,"Maria",8,"C"),(5,"Agelos",7,"D"),(6,"Katerina",9,"E"),(7,"Dhmhtra",9,"C")]

header =["id","Student_Name","Grade","Class"]

std_dataF = spark.createDataFrame(Students_data,header)

std_dataF.show()
std_dataF.filter(std_dataF.Class=="A").show()
