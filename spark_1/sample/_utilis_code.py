# Spark_1 Assignment main code

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# create the session
def Session():
    spark = SparkSession.builder.master("local").appName("sample").getOrCreate()
    return spark

# read the user csv file
def user(spark):
    user1 = spark.read.option('header', True).csv(r"C:\Users\HARI\Downloads\assign_files\user.csv", inferSchema=True)
    return user1


# read the transcation csv file
def transaction(spark):
    trans = spark.read.option('header', True).csv(r"C:\Users\HARI\Downloads\assign_files\transaction.csv", inferSchema=True)
    return trans


# join the files - inner join
def join_files(user1,trans):
    join = user1.join(trans)
    return join


# products bought by each user
def product_bought(j):
    return j.groupBy("userid","product_description").count()


# Total spending done by each user on each product
def total_spending(j):
    return j.groupBy("userid","product_description").sum("price")


# Count of unique locations where each product is sold
def uniq_loc(j):
    return j.groupBy("location ","product_description").count()