# Importing all required pyspark libraries and functions
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F

# The spark session is configured here
conf = SparkConf().setAppName("covid19").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("Error")
spark = SparkSession(sc)

# The covid19.csv file is read with header=True to interpret the headings
covidDF = spark.read.csv("C:\spark\covid19.csv",header=True)
print("")
print("")
print("---------------------------------------------------------------------------------")
print("Coursework 1 Big Data Analytics - Sam Tribe - 201318996")
print("---------------------------------------------------------------------------------")
print("")
print("")
print("---------------------------------------------------------------------------------")
print("covid19.csv read as Dataframe with header=True")
print("---------------------------------------------------------------------------------")
covidDF.show()
print("---------------------------------------------------------------------------------")
print("Schema for dataframe")
print("---------------------------------------------------------------------------------")
covidDF.printSchema()
print("---------------------------------------------------------------------------------")
print("Input dataframe row count: " + str(covidDF.count()))
print("---------------------------------------------------------------------------------")

print("Filtering out NULL values from df")
covidDF = covidDF.na.drop()
covidDF.show()

print("---------------------------------------------------------------------------------")
print("Highest deaths per country")
print("---------------------------------------------------------------------------------")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()

print("---------------------------------------------------------------------------------")
print("Highest cases per country sorted by highest first")
print("---------------------------------------------------------------------------------")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(max)')).orderBy('total_cases(max)',ascending=False).show()

print("---------------------------------------------------------------------------------")
print("lowest cases per country sorted by lowest first")
print("---------------------------------------------------------------------------------")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(min)')).orderBy('total_cases(min)',ascending=True).show()
