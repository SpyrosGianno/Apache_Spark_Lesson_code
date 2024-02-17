#ypologismos mesou orou vathmon mathitwn & epistrofh megalhterou

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Grades_student").getOrCreate()

StudentData = ([("Maria",8),("Kostas",7),("Nikoleta",10),("Giannis",5),("Panagiotis",6)])

dfStud = spark.createDataFrame(StudentData, ["Name","Grade"])

dfStud.show()
# ypologizei ton arithmo twn mathitwn
studCount = dfStud.count()
print(studCount)

# ypologizei ton arithmo twn mathitwn 
MessOS = dfStud.rdd.map(lambda x: x[1]).reduce(lambda x,y: x+y)
#Mas epistrefei ton pososto epityxias (mesos oros)
print(MessOS/studCount)

# Anazhtaei thn megaliteri vathmologia
bigGrate = dfStud.rdd.map(lambda x: x[1]).reduce(lambda x,y: x if x > y else y)
# Mas epistrefei ton megalitero vathmo
print(bigGrate)
