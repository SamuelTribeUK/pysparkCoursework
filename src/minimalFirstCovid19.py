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
print("Filtering out NULL values from dataframe")
covidDF = covidDF.filter(covidDF.continent.isNotNull() & covidDF.location.isNotNull() & covidDF.date.isNotNull() & covidDF.total_cases.isNotNull() & covidDF.new_cases.isNotNull() & covidDF.total_deaths.isNotNull() & covidDF.new_deaths.isNotNull())
covidDF.show()
print("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()
print("max and min function results on total_cases")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases_max'), F.min(covidDF.total_cases).alias('total_cases_min')).show()
