# Samuel Tribe - 201318996 - S.Tribe@student.liverpool.ac.uk
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DateType
conf = SparkConf().setAppName("covid19").setMaster("local")
spark = SparkSession(SparkContext(conf=conf))
csvPath = "C:\spark\COMP336-Coursework-1\data\covid19.csv"
covidDF = spark.read.csv(csvPath,header=True,inferSchema=True)
covidDF = covidDF.withColumn("date", F.col("date").cast(DateType()))
print("covid19.csv read as Dataframe with header=True")
covidDF.show()
print("Schema for dataframe")
covidDF.printSchema()
print("Input dataframe row count: " + str(covidDF.count()))
print("Filtering out NULL values from dataframe")
covidDF = covidDF.na.drop()
covidDF.show()
print("null-dropped dataframe row count: " + str(covidDF.count()))
print("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()
covidDF = covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(max)'))
print("Highest cases per country sorted by highest first")
covidDF.orderBy('total_cases(max)',ascending=False).show()
print("lowest cases per country sorted by lowest first")
covidDF.orderBy('total_cases(max)',ascending=True).withColumnRenamed('total_cases(max)','total_cases(min)').show()
