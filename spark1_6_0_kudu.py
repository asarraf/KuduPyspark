# Dated           : Aug 25, 2017
# About           : Sample Pyspark (1.6.0) code to count number of Rows in a Kudu Table
# Pyspark Version : 1.6.0
# Kudu Version    : 1.2.0
# Coder           : Ankit Sarraf

import ConfigParser
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# Initialize the configuration
conf = (SparkConf().setMaster("yarn-client").setAppName("KuduSpark_AS"))

# Initialize the SparkContext using the above conf
sc = SparkContext(conf = conf)

# Initialize the SQLContext using the above SparkContext
sqlContext = SQLContext(sc)

# Use the ConfigParser
configParser = ConfigParser.ConfigParser()
# Load the file that contains the Configuration Parameters
configParser.read('kudu_configuration.conf')

# Determine the kudu_master as provided in the Config File
kudu_master = configParser.get('Kudu', 'KuduMaster')
# Provide the table to be read
kudu_table = configParser.get('Kudu', 'KuduTable')

# Display Data Read from Config File
print('KUDU MASTER: ' + kudu_master)
print('KUDU TABLES: ' + kudu_table)

# Load the table in the memory
kudu_events_df = sqlContext.read.format('org.apache.kudu.spark.kudu'). \
        option('kudu.master', kudu_master). \
        option('kudu.table', kudu_table). \
        load()

# Display the count of the rows in the table
print('My Count: ' + str(kudu_events_df.count()))
