from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sum").getOrCreate()

data = ([(147,"Leonidas","Makridakis",2500),(135,"Maria","Nikolaou",8500),(138,"Thanasis","Eteoklis",1200),(148,"Panagiotis","Tasou",6100),(180,"Katerina","Filipaki",930),(178,"Nikoleta","Rani",2200),(200,"Vagelis","Xasapis",3400),(184,"Giota","Petropoulou",1980),(581,"leonidas","Livizakis",1225)]) 

column = ["id","firstname","lastname","salary"]

df = spark.createDataFrame(data,column)
df.show()

sallSum = df.agg({df.columns[3]: "sum"})
sallSum.show()

salMin = df.agg({df.columns[3]: "min"})
salMin.show()

salMax = df.agg({df.columns[3]: "max"})
salMax.show()

salAvg = df.agg({df.columns[3]: "avg"})
salAvg.show()
