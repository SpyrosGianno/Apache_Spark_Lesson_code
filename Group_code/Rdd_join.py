from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rdd_join").getOrCreate()

student_grade =([(1252,"Eleni"),(1288,"Nikos"),(1389,"Giannis"),(1411,"Maria"),(1512,"Kostas"),(1238,"Panagiotis"),(1820,"Marios"),(1722,"Nikoleta"),(1258,"Leonidas")])

student_math = ([(1252,"Mathimatika",8),(1288,"Glosa",9),(1389,"Mathimatika",6),(1411,"Glosa",10),(1512,"Mathimatika",8),(1238,"Glosa",7),(1820,"Mathimatika",5),(1722,"Glosa",9),(1258,"Glosa",7)])

columst = ["id","name"]
colums2 = ["id","Mathima","grade"]

#dhmiourgia twn rdd
studGrade1 = spark.sparkContext.parallelize(student_grade).toDF(columst)
studGrade2 = spark.sparkContext.parallelize(student_math).toDF(colums2)

for std1 in studGrade1.collect():
   print(std1)

for std2 in studGrade2.collect():
   print(std2)

#Klish sinartishs join
studJoin = studGrade1.join(studGrade2,studGrade1.id == studGrade2.id)

for std3 in studJoin.collect():
      print(std3)
