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
import time


# decorator "timer". use before function definition: @timer
def timer(f):
    def tmp(*args, **kwargs):
        t = time.time()
        res = f(*args, **kwargs)
        print("Task execution time: %f" % (time.time()-t))
        return res

    return tmp


# load ProdID/ProdName from csv file as dataframe
def load_plan_names():
    csv_df = spark.read.csv('./plans.csv', header=True)
    plan_names_df = csv_df.select("product_id", concat_ws(" ", "product_type", "product_name").alias("product"))
    return plan_names_df


@timer
def broadcast_join():
    # Add a 'product' column using broadcasted dataframe loaded from csv file
    dataWithPlanNames = rawDF.join(broadcast(load_plan_names()), "product_id")
    dataWithPlanNames.show(10, False)


@timer
def broadcast_mapping():
    plan_names_df = load_plan_names()
    plan_names_df.show(1, False)
    spark.sparkContext.broadcast(plan_names_df)



if __name__ == '__main__':
    # Start spark session
    spark = SparkSession.builder.appName("PopulatePlanNames").getOrCreate()

    # Load up data as dataframe
    rawDF = spark.read.parquet("./raw_data.parquet")
    rawDF.show(10, False)

    # First - use broadcast join
    broadcast_join()

    # Second = broadcast mapping before join
    broadcast_mapping()


    # Stop the session
    spark.stop()
