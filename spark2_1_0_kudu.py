# Dated .         : Aug 26, 2017
# About           : Sample Pyspark (2.1.0) code to count number of Rows in a Kudu Table
# Pyspark Version : 2.1.0
# Kudu Version    : 1.2.0
# Coder           : Ankit Sarraf

import ConfigParser
from pyspark.sql import SparkSession

# from pyspark.sql import SparkContext
# from pyspark import SparkConf
# from pyspark.sql import SQLContext


# Initialize the configuration
#conf = (SparkConf().setMaster("yarn-client").setAppName("KuduSpark_AS"))

spark = SparkSession.builder \
        .appName('KuduSpark_2.1') \
        .enableHiveSupport() \
        .getOrCreate()


configParser = ConfigParser.ConfigParser()
configParser.read('test.conf')

# Determine the kudu_master as provided in the Config File
kudu_master = configParser.get('Kudu', 'KuduMaster')
# Provide the table to be read
kudu_table = configParser.get('Kudu', 'KuduTable')

# Display the data read from the Config File
print('KUDU MASTER: ' + kudu_master)
print('KUDU TABLES: ' + kudu_table)

# Load the data in the memory
kudu_data_df = spark.read.format('org.apache.kudu.spark.kudu'). \
        option('kudu.master', kudu_master). \
        option('kudu.table', kudu_table). \
        load()

# Display the count of rows in the table
print('My Count: ' + str(kudu_events_df.count()))
