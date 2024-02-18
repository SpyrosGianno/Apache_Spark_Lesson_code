from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFr_join").getOrCreate()

student_grade =([(1252,"Eleni"),(1288,"Nikos"),(1389,"Giannis"),(1411,"Maria"),(1512,"Kostas"),(1238,"Panagiotis"),(1820,"Marios"),(1722,"Nikoleta"),(1258,"Leonidas")])

student_math = ([(1252,"Mathimatika",8),(1288,"Glosa",9),(1389,"Mathimatika",6),(1411,"Glosa",10),(1512,"Mathimatika",8),(1238,"Glosa",7),(1820,"Mathimatika",5),(1722,"Glosa",9),(1258,"Glosa",7)])

columst = ["id","name"]
colums2 = ["id","Mathima","grade"]

#dhmiourgia twn rdd
studGrade1 = spark.createDataFrame(student_grade,columst)
studGrade2 = spark.createDataFrame(student_math,colums2)

print(studGrade1.show())
print(studGrade2.show())

#Klish sinartishs join
studJoin = studGrade1.join(studGrade2,studGrade1.id == studGrade2.id)

print(studJoin.show())

studSort = studJoin.sort(studJoin.grade)
stdCount = studSort.count()

print(studSort.show())
print(stdCount)
