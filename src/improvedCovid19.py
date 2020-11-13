# Samuel Tribe - 201318996 - S.Tribe@student.liverpool.ac.uk
# This is the full version of the improved pyspark script with user-friendly
# printing and labels and comments to make the data easier to understand. For a
# more direct interpretation of the instructions see firstCovid19.py
# For a stripped down version of this code (without the separatorText function
# and the comments) see minimalImprovedCovid19.py

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
# The dataframe is shown and the schema is printed to show the data has been
# loaded correctly
separatorText("covid19.csv read as Dataframe with header=True")
covidDF.show()
separatorText("Schema for dataframe")
covidDF.printSchema()
# The number of rows in the dataframe is printed here (this is optional, not
# required in the coursework but it is useful for seeing the changes after na
# drop)
separatorText("Input dataframe row count: " + str(covidDF.count()))

# The null values are dropped from the dataframe here (using na.drop rather than
# filter after asking Dr. Amen if this was appropriate). The resulting Dataframe
# values are shown here and the new count is printed too
separatorText("Filtering out NULL values from dataframe")
covidDF = covidDF.na.drop()
covidDF.show()
separatorText("null-dropped dataframe row count: " + str(covidDF.count()))

# The highest deaths per country are calculated and shown here using the groupBy
# function to group by location. Then the aggregate function gets the maximum
# values from the covidDF total_deaths column for each location
separatorText("Highest deaths per country")
covidDF.groupBy(['location']).agg(F.max(covidDF.total_deaths)).show()

# The maximum total cases per country are calculated here and saved back to
# covidDF. The groupBy and aggregate functions are almost identical to the
# highest deaths per country above, except the column name for total cases is
# renamed to 'total_cases_max' ready for the next operation
covidDF = covidDF.groupBy(['location']).agg(F.max(covidDF.total_cases).alias('total_cases(max)'))

# The dataframe calculated above is ordered by total_cases(max) in descending
# order so the countries with the highest number of total_cases is shown first
separatorText("Highest cases per country sorted by highest first")
covidDF.orderBy('total_cases(max)',ascending=False).show()

# The dataframe calculated above is ordered by total_cases(max) in ascending
# order this time so the countries with the lowest number of total_cases is
# shown first (lowest number of up to date total_cases)
separatorText("lowest cases per country sorted by lowest first")
covidDF.orderBy('total_cases(max)',ascending=True).withColumnRenamed('total_cases(max)','total_cases(min)').show()
