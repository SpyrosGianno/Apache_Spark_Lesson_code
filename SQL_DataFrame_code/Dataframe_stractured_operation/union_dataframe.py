from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("union_dataframe").getOrCreate()

df = spark.read.csv("studGrade.csv", header=True)
studf = spark.read.csv("studentGrade.csv",header = True )
  
studf.show()
df.show()
# pragmatopieitai h enwsh twn dyo dataframe
unionStud = df.union(studf)
unionStud.show()

#pragmatopoieitai h anazhthsh ths thmhs glossa
studWhere = unionStud.where(unionStud.Lesson =="Glossa").distiinct()
studWhere.show()
