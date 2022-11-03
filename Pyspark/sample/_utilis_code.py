# Pyspark Assignment

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime,date

# create the session
def Session():
    spark = SparkSession.builder.master("local").appName("pyspark_2").getOrCreate()
    return spark

#create a table 1

def table_1(spark):
    table1=spark.read.option("header",True).csv(r"C:\Users\HARI\Downloads\assign_files\Table1.csv",inferSchema=True)
    return table1


# Convert the Issue Date with the timestamp format and timestamp to date
def convert_date(t1):
    d= t1.withColumn("timestamp",from_unixtime(col("Issue Date")/1000)).withColumn("datetype",to_date(col("timestamp")))
    return d

# Remove the starting extra space in Brand column for LG and Voltas fields
def remove_space(d1):
    rs= d1.withColumn("Brand",trim(col("Brand")))
    return rs


# Replace null values with empty values in Country column
def update_table(space_trim):
    u1 = space_trim.fillna(value="",subset="Country")
    return u1

# create table 2
def table_2(spark):
    table2 = spark.read.option("header",True).csv(r"C:\\Users\HARI\Downloads\assign_files\Table2.csv",inferSchema=True)
    return table2


# change camelcase to snake case
def change_case(t2):
    Table2 = t2.withColumnRenamed('SourceId','source_id').withColumnRenamed('TransactionNumber','transaction_number').\
        withColumnRenamed('Language','language').withColumnRenamed('ModelNumber','model_number').\
        withColumnRenamed('StartTime','start_time').withColumnRenamed('ProductNumber','product_number')
    return Table2



# another column as start_time_ms and convert the values of StartTime to milliseconds.
def convert_time(T2):
    w = T2.withColumn('start_time_ms',unix_timestamp(col('start_time')))
    return w


# combine table 1 and table 2

def combine_table(u1,u2):
    inner_join = u1.join(u2, u1.product_number == u2.product_number,"inner")
    return inner_join


# get the country as EN
def get_EN(ct):
    df2 = ct.where(ct.Country == "EN")
    return df2