# Samuel Tribe - 201318996 - S.Tribe@student.liverpool.ac.uk
# This pyspark script is the literal interpretation of the instructions in the
# coursework document, however my full adaptation with more appropriate code
# is in covid19.py
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

conf = SparkConf().setAppName("covid19").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

csvPath = "C:\spark\COMP336-Coursework-1\data\covid19.csv"

def separatorText(heading):
    print("---------------------------------------------------------------------------------")
    print(heading)
    print("---------------------------------------------------------------------------------")

separatorText("Coursework 1 Big Data Analytics - Samuel Tribe - 201318996")

covidDF = spark.read.csv(csvPath,header=True,inferSchema=True)
covidDF = covidDF.withColumn("date", F.col("date").cast(DateType()))

separatorText("covid19.csv read as Dataframe with header=True")
covidDF.show()
separatorText("Schema for dataframe")
covidDF.printSchema()

separatorText("Input dataframe row count: " + str(covidDF.count()))

separatorText("Filtering out NULL values from dataframe")
covidDF = covidDF.filter(covidDF.continent.isNotNull() & covidDF.location.isNotNull() & covidDF.date.isNotNull() & covidDF.total_cases.isNotNull() & covidDF.new_cases.isNotNull() & covidDF.total_deaths.isNotNull() & covidDF.new_deaths.isNotNull())
covidDF.show()
separatorText("null-dropped dataframe row count: " + str(covidDF.count()))

separatorText("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()

separatorText("Highest cases per country sorted by highest first")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(max)')).orderBy('total_cases(max)',ascending=False).show()

separatorText("min function for total_cases")
covidDF.groupBy(['location']).agg(F.min(covidDF.total_cases).alias('total_cases(min)')).orderBy('total_cases(min)',ascending=True).show()
