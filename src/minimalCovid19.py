# Samuel Tribe - 201318996 - S.Tribe@student.liverpool.ac.uk
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
conf = SparkConf().setAppName("covid19").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sc.setLogLevel("Error")
csvPath = "C:\spark\COMP336-Coursework-1\data\covid19.csv"
covidDF = spark.read.csv(csvPath,header=True,inferSchema=True)
covidDF.show()
covidDF.printSchema()
covidDF = covidDF.na.drop()
print("Filtering out NULL values from dataframe")
covidDF.show()
print("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()
print("Highest cases per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(max)')).orderBy('total_cases(max)',ascending=False).show()
print("Lowest cases per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(min)')).orderBy('total_cases(min)',ascending=True).show()
