"""
@author: Bor

data files:

plans.csv - used as broadcasted mapping
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
# from pyspark.sql import functions as func
from pyspark.sql.functions import concat_ws, broadcast


# load ProdID/ProdName from csv file as dataframe
def load_plan_names():
    csv_df = spark.read.csv('./plans.csv', header=True)
    plan_names_df = csv_df.select("product_id", concat_ws(" ", "product_type", "product_name").alias("product"))
    return plan_names_df


# Main entrypoint. Start spark session
# local:
spark = SparkSession.builder.appName("PopulatePlanNames").getOrCreate()

# Load up data as dataframe
# to load data from S3 use: s3n://bucket_name/file.ext
rawDF = spark.read.parquet("./raw_data.parquet")

# Grab the top 10
rawDF.show(10, False)

# Add a 'product' column using broadcasted dataframe loaded from csv file
dataWithPlanNames = rawDF.join(broadcast(load_plan_names()), "product_id")

# Grab the top 10
dataWithPlanNames.show(10, False)

# Stop the session
spark.stop()
