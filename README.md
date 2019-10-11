# datalake_kafka_spark_ingestion
Main class: readKafkaToHdfs.scala

Function:
1. consume data from kafka
2. for each topic and each partition write message into HDFS
3. for each RDD commit offset to hbase table
4. commit message summary information to hbase table for audit control purpose
