from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_Dataframe_distinct").getOrCreate()

Students_data = [(1,"giorgos",8,"A"),(2,"elpida",6,"A"),(3,"dhmhtra",8,"C"),(4,"maria",8,"C"),(5,"agelos",7,"D"),(6,"katerina",9,"E"),(3,"dhmhtra",8,"C")]

header =["id","Student_Name","Grade","Class"]

std_dataF = spark.createDataFrame(Students_data,header)

std_dataF.show()

std_dataF.distinct().show()
