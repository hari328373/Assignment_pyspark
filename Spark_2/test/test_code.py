# Databricks notebook source
# MAGIC %run ../core/sample_code

# unittesting for Saprk_2 Assignment

import unittest

class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = createSparkSession()
        cls.spark = spark
        
    def test_create_torrent(self):
        ghtorrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details","dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
        count = torrent_df.count()
        self.assertGreatEqual(count, 1)

# COMMAND ----------

 def test_req_Col(self):
        ghtorrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
        ghtorrent_df_extract = req_Col(ghtorrent_df)
        actual_output = ghtorrent_df_extract.columns
        expected_output = ['LogLevel', 'timestamp', 'ghtorrent_client_id', 'downloader_id', 'repository_torrent', 'Request_status_ext', 'Request_status', 'request_url']
        self.assertEqual(actual_output,expected_output)

# COMMAND ----------

 def test_count_lines(self):
        ghtorrent_df = (self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
        no_of_lines = ghtorrent_df.count()
        self.assertEqual(no_of_lines, 54)


def test_WARN_count(self):
    ghtorrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
    expected_count =  WARN_count(torrent_df, "LogLevel", "WARN")["warn_count"]
    self.assertEqual(expected_count, 3811)

def test_ApiClient(self):
    ghtorrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
    torrent_df_extract = req_Col(ghtorrent_df)
    expected_count = rep_api(torrent_df_extract, "api_client.rb", "repository_torrent")["api_client_repo_count"]
    self.assertEqual(expected_count,37595)


def ClientReq_http(self):
    ghtorrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
    torrent_df_extract = split_req_Col(torrent_df)
    expected_ouput = ClientReq_http(torrent_df_extract, "request_url", "ghtorrent_client_id").first()["ghtorrent_client_id"]
    actual_output = " ghtorrent-5 "
    self.assertEqual(expected_ouput,actual_output)


def testGetFailed_fail_http(self):
    torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
    torrent_df_extract = split_req_col(torrent_df)
    expected_output = testGetFailed_fail_http(torrent_df_extract, "Request_status_ext", "%Failed%", "ghtorrent_client_id",   "max_failed_req_client")["max_failed_req_client"]
        actual_output = " ghtorrent-13 "
        self.assertEqual(expected_output,actual_output)


 def test_A_hrs(self):
        torrent_df =  create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
        torrent_df_extract = split_req_col(torrent_df)
        expected_output = test_A_hrs(torrent_df_extract).first()["active_hours"]
        actual_output = 16
        self.assertEqual(expected_output,actual_output)



 def test_active_repos(self):
        torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                    "dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/ghtorrent_logs.txt")
        torrent_df_extract = split_req_col(torrent_df)
        expected_output = countrepo(torrent_df_extract, "repository_torrent", "active_repo_used").first()["active_repo_used"]
        actual_output = "ghtorrent.rb"
        self.assertEqual(expected_output,actual_output)
        
        @classmethod
        def tearDownClass(cls):
            cls.spark.stop()


