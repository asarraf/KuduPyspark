# Dated .         : Sept 05, 2017
# About           : General Template to Access Kudu Tables from Spark2
# Pyspark Version : 2.1.0
# Kudu Version    : 1.2.0
# Coder           : Ankit Sarraf

import sys
import ConfigParser
from pyspark.sql import SparkSession

appName = sys.argv[1]
kudu_master = sys.argv[2]
kudu_tables = sys.argv[3:len(sys.argv)-1]

# Build SparkSession
spark = SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

# Create Temporary tables
for i in range(0, len(kudu_tables)):
  # Create the kudu_data_df
  kudu_data_df = spark.read.format('org.apache.kudu.spark.kudu'). \
    option('kudu.master', kudu_master). \
    option('kudu.table', kudu_table). \
    load()
  # Create or Replace a Temp View in Pyspark for executing sql queries
  kudu_data_df.createOrReplaceTempView("table_" + str(i))

# Count number of rows in the Table
spark.sql(sys.argv[len(sys.argv)]).show()
