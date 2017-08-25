# KuduPyspark
Sample Code to get started with KuduPyspark

Instructions:
- The latest JAR file for Kudu-Spark can be downloaded from the link Maven Website
- The Jar file used for this program is downloaded from https://repository.cloudera.com/content/repositories/releases/org/apache/kudu/kudu-spark_2.10/1.2.0-cdh5.10.0/kudu-spark_2.10-1.2.0-cdh5.10.0.jar
- Running instructions
  - pyspark shell:
    Start the pyspark2 shell with --jars: pyspark2 --jar kudu-spark_2.10-1.2.0.jar (Absolute/Relative Path)
  - Spark submit
    spark2-submit --jars kudu-spark_2.10-1.2.0.jar (Absolute/Relative Path) pyspark-kudu_application_path.py
