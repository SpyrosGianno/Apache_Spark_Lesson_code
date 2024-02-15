from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("student_filter").getOrCreate()

students_data ="Giorgos 8 A ,Elpida 6 A,Giannis 8 c,Aggelos 8 A,Marija 9 B,Anna 9 A".split(",")

student_Rdds = spark.sparkContext.parallelize(students_data)

student_names = student_Rdds.collect()

for std in student_names: print(std)

# Epistrefei ta stixeia poy xekinoun me to stixeio-gramma
def stdStartsWith(std,stixeio): return std.startswith(stixeio)
student_Rdds.filter(lambda std: stdStartsWith(std,"Gi") ).collect()

# Epistrefei ta stixeia poy teleionoun sto stixeio-gramma
def stdEnds(std,stixeio): return std.endswith(stixeio)
student_Rdds.filter(lambda std: stdEnds(std,"Gi") ).collect()
