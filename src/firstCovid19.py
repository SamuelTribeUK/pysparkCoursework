# Samuel Tribe - 201318996 - S.Tribe@student.liverpool.ac.uk
# This pyspark script is the literal interpretation of the instructions in the
# coursework document, however my full adaptation with more appropriate code
# is in improvedCovid19.py
# For a stripped down version of this code (without the separatorText function
# and the comments) see minimalFirstCovid19.py

# Importing all required pyspark libraries and functions
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# The spark session is configured here
conf = SparkConf().setAppName("covid19").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# The path of the csv data file is specified here, change this string for use on
# different OS or machines
csvPath = "C:\spark\COMP336-Coursework-1\data\covid19.csv"

# separatorText prints the headings between separator lines for a more
# user-friendly experience reading the results
def separatorText(heading):
    print("---------------------------------------------------------------------------------")
    print(heading)
    print("---------------------------------------------------------------------------------")

separatorText("Coursework 1 Big Data Analytics - Samuel Tribe - 201318996")

# The covid19.csv file is read with header=True and inferSchema=True (to ensure
# correct dataTypes)
covidDF = spark.read.csv(csvPath,header=True,inferSchema=True)
covidDF = covidDF.withColumn("date", F.col("date").cast(DateType()))

separatorText("covid19.csv read as Dataframe with header=True")
covidDF.show()
separatorText("Schema for dataframe")
covidDF.printSchema()

# The null values are dropped from the dataframe here using the specified spark
# filter function. The resulting Dataframe values are shown here
separatorText("Filtering out NULL values from dataframe")
covidDF = covidDF.filter(covidDF.continent.isNotNull() & covidDF.location.isNotNull() & covidDF.date.isNotNull() & covidDF.total_cases.isNotNull() & covidDF.new_cases.isNotNull() & covidDF.total_deaths.isNotNull() & covidDF.new_deaths.isNotNull())
covidDF.show()

# The highest deaths per country are calculated and shown here using the groupBy
# function to group by location. Then the aggregate function gets the maximum
# values from the covidDF total_deaths column for each location
separatorText("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()

# The highest and lowest total_cases values are aggregated and grouped by
# location here and printed to the console
separatorText("max and min function results on total_cases")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases_max'), F.min(covidDF.total_cases).alias('total_cases_min')).show()
