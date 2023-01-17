# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, count, hour, minute, second


def createSession():
    s=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
    return s
spark=createSession()


def create_Torrent_df(spark,col1,rename_col_1,col2,rename_col_2,col3,rename_col_3, path):
    df = spark.read.csv(path)
    ghtorrent_df = df.withColumnRenamed(col1,rename_col_1).withColumnRenamed(col2,rename_col_2).withColumnRenamed(col3,rename_col_3)
    return ghtorrent_df
df1 = create_Torrent_df(spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2",
                              "ghtorrent_details","dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")


# converting structure of logfile 

def req_Col(ghtorrent_log_df):
    api_cliendId= ghtorrent_log_df .withColumn("ghtorrent_client_id", split(col("ghtorrent_details"), "--").getItem(0)) \
        .withColumn("repository", split(col("ghtorrent_details"), "--").getItem(1)) \
        .withColumn("downloader_id", split(col("ghtorrent_client_id"), "-").getItem(1)) \
        .withColumn("repository_torrent", split(col("repository"), ':').getItem(0)) \
        .withColumn("Request_status_ext", split(col("repository"), ':').getItem(1)) \
        .withColumn("Request_status", split(col("Request_status_ext"), ",").getItem(0)) \
        .drop(col("repository")) \
        .withColumn("request_url", split(col("ghtorrent_details"), "URL:").getItem(1)) \
        .drop(col("ghtorrent_details"))
    return api_cliendId

ghtorrent_df = req_Col(df1)
print("Printing the torrent df")
ghtorrent_df.show(truncate=False)


# COMMAND ----------


#  How many lines does gh_torrent contain
def count_lines(df1):
    count_line = df1.agg(count("*").alias("total_count_lines"))
    count_line.sort("total_count_lines")
    return count_line

total_count = count_lines(ghtorrent_df)
print("Total number of lines")
total_count.show()


# COMMAND ----------

# count the number of WARNing messages
def WARN_count(df1):
    wranlog = df1.filter("LogLevel = 'WARN'").agg(count("*").alias("totalWARN_count"))
    wranlog.sort("totalWARN_count")
    return wranlog
num_WARN = WARN_count(df1)
print("number of WARNing")
num_WARN.show()


# COMMAND ----------

# How many repositories where processed in total? Use the api_client lines only
def api_client(df1):
    rep_api = df1.filter(col("ghtorrent_details").like("%api_client.rb%")).agg(count("*").alias("api_claim"))
    rep_api.sort("api_claim")
    return rep_api
api= api_client(df1)
print("api_cilent repositories")
api.show()


# COMMAND ----------

# which client did most HTTP requests?
def http_req(df1):
    http_req = df1.groupBy("ghtorrent_details").agg(count("*").alias("http_req_count"))
    http_req.sort("http_req_count")
    return http_req
http = http_req(df1)
print("active http requests")
http.show(truncate=False)

# COMMAND ----------

# Which client did most FAILED HTTP requests? Use group by to provide an answer
def fail_http(df1):
    Failed_req = df1.filter(col("ghtorrent_details").like("%Failed%")).groupBy("ghtorrent_details").agg(count("*").alias("Failed_request"))
    Failed_req.sort("Failed_request")
    return Failed_req
fail_https=fail_http(df1)
print("FAILED HTTP requests")
fail_https.show(truncate=False)


# COMMAND ----------

# What is the most active hour of day
def active_hr(df1):
    active_hr = df1.withColumn("hour", hour(col("timestamp"))).groupBy("hour").agg(count("*").alias("active_hours"))
    active_hr.sort("active_hours")
    return active_hr
ac_hr = active_hr(df1)
print("most active hour of day")
ac_hr.show(truncate=False)



# COMMAND ----------

# count of Active repos
def active_repos(df1):
    active_repository = df1.groupBy("ghtorrent_details").agg(count("*").alias("active_repository"))
    active_repository.sort("active_repository")
    return active_repository
ac_re = active_repos(df1)
print("count of Active repos")
ac_hr.show(truncate=False)


