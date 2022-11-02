"""
@author: Bor

data files:

plans.csv - used as broadcasted dictionary
    product_id,"product_type","product_name"

raw_data.parquet - data to process
    employee_id (autoincremental),
    employer_id (int 1-20),
    product_id (int 1-15),
    covered_persons (int 1-5),
    start_date (ISO 946681200 - 2524604400),
    end_date (ISO 946681200 - 2524604400)

to run on master node:
spark-submit --executor-memory 1g mainjob.py
"""


from pyspark.sql import SparkSession
# from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
import codecs


# load ProdID/ProdName from ext file as dict
def loadPlanNames():
    PlanNames = {}
    with codecs.open("./plans.csv", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split(',')
            PlanNames[int(fields[0])] = fields[1].strip('\"') + ' ' + fields[2].strip().strip('\"')
    return PlanNames


def lookupName(productID):
    return nameDict.value[productID]


# Main entrypoint. Start spark session
# local:
spark = SparkSession.builder.appName("PopulatePlanNames").getOrCreate()

# Broadcast dictionary
nameDict = spark.sparkContext.broadcast(loadPlanNames())

# Load up data as dataframe
# to load data from S3 use: s3n://bucket_name/file.ext
rawDF = spark.read.parquet("./raw_data.parquet")

# Grab the top 10
rawDF.show(10, False)

# Create a user-defined function to look up names from broadcasted dictionary
lookupNameUDF = func.udf(lookupName)

# Add a PlanName column using new udf
dataWithPlanNames = rawDF.withColumn("PlanName", lookupNameUDF(func.col("product_id")))

# Grab the top 10
dataWithPlanNames.show(10, False)

# Stop the session
spark.stop()
