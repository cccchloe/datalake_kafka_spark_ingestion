package com.marlabs.datalake

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession,}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType}
import org.apache.log4j.LogManager

object HbaseOffsetManagement {

  def updateKafkaOffsetDetails(OffsetDF: DataFrame): Unit = {
    val logger = LogManager.getLogger(getClass.getName)
    try {
      //write offset value into management table
      val hbaseCatalog = getOffsetTableCatalog()
      //get latest offset per partitionId
      val latestOffsetDF = OffsetDF
        .groupBy(col("partitionId").as("partition")).agg(max(col("offset")).as("maxoffset"))
        .select(col("partition"), col("maxoffset"))
      latestOffsetDF
        .join(OffsetDF, (latestOffsetDF("partition") === OffsetDF("partitionId") && latestOffsetDF("maxoffset")))
        //rowkey design: topicName~groupId~messageDate~timeStamp
        .withColumn("rowkey",
          concat(col("topicName"), lit("~"),
            col("groupId"), lit("~"),
            col("date"), lit("~"),
            col("timeStamp")))
        .select(col("rowkey"),
          col("partition").as("partitionId"),
          col("maxoffet").as("offsetValue"),
          col("timeStamp").as("uniqueBatchId"))
        .write
        .options(Map(HBaseTableCatalog.tableCatalog -> hbaseCatalog, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase").save()
    } catch {
      case e: Exception => {
        println(e)
        throw e
      }
    } finally {

    }
  }

  def updateKafkaOffsetAudit(OffsetDF: DataFrame): Unit = {
    val logger = LogManager.getLogger(getClass.getName)
    try {
      //write offset summary info into audt table
      val auditCatalog = getAuditTableCatalog()
      val AuditDF = OffsetDF
        .withColumn("rowkey",
          concat(col("topicName"), lit("~"),
            col("groupId"), lit("~"),
            col("date"), lit("~"),
            col("timeStamp")))
        .groupBy(col("rowkey"), col("date"))
        .agg(count(col("offset")).cast(StringType).as("countoffset"))
        .select(col("rowkey"), col("countoffset"), col("date"))

      AuditDF.write
        .options(Map(HBaseTableCatalog.tableCatalog -> auditCatalog, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase").save()
    } catch {
      case e: Exception => {
        println(e)
        throw e
      }
    } finally {
    }
  }


  def getKafkaOffsetDetails(spark: SparkSession): DataFrame = {

    val logger = LogManager.getLogger(getClass.getName)
    try {
      val sqlcontext = spark.sqlContext
      import sqlcontext.implicits._
      logger.info("@@@ Entered getKafkaOffsetDetails")

      val hbaseCatalog = getOffsetTableCatalog()
      val hbaseTableDF = sqlcontext.read
        .options(Map(HBaseTableCatalog.tableCatalog -> hbaseCatalog))
        .format("org.apache.spark.sql.execution.datasources.hbase").load()

      val kafkaOffsetDetailDF = hbaseTableDF.withColumn("_tmp", split($"rowkey", "\\~")).select(
        $"_tmp".getItem(0).as("Topic"),
        $"partitionId",
        $"_tmp".getItem(3).as("timeStamp"),
        $"offsetValue"
      ).drop(col("_tmp"))

      logger.info("@@@ Prepared offsetdf successfully")
      kafkaOffsetDetailDF
    } catch {
      case e: Exception => {
        println(e)
        throw e
      }
    } finally {
    }
  }


  def getFromOffsetDetails(kafkaOffsetDetailsDF: DataFrame): Map[TopicPartition, Long] = {

    val logger = LogManager.getLogger(getClass.getName)

    try {
      logger.info("@@@ Preparing latest offsets DF")
      val fromOffsets = collection.mutable.Map[TopicPartition, Long]()

      val latestOffsetsDF = kafkaOffsetDetailsDF
        .groupBy(col("Topic"), col("partitionId"))
        .agg(max(col("offetVaue")))

      logger.info("@@@ Preparing fromoffsets from latestoffsetdf")
      for (row <- latestOffsetsDF.rdd.collect()) {
        val topic: String = row(0).toString
        val partition: Int = row(1).toString.toInt
        val offset: Long = row(2).toString.toLong
        fromOffsets += (new TopicPartition(topic, partition) -> offset)
        logger.info("@@@ fromOffsets" + fromOffsets.toString())
      }
      fromOffsets.toMap
    } catch {
      case e: Exception => {
        println(e)
        throw e
      }
    } finally {
    }
  }


  def getOffsetTableCatalog(): String = {
    s"""{
       |"table":{"namespace":"DATALAKE","name":"STREAMING_OFFSET_MGMT"},
       |"rowkey":"row",
       |"columns":{
       |"rowkey":{"cf":"rowkey","col":"row","type":"string"},
       |"partitionId":{"cf":"offsets","col":"partitionId","type":"string"},
       |"offetVaue":{"cf":offsets","col":"offsetValue","type":"string"},
       |"uniqueBatchId":{"cf":"offsets","col":"uniqueBatchId","type":"string"}
       |}
       |}""".stripMargin
  }

  def getAuditTableCatalog(): String = {
    s"""{
       |"table":{"namespace":"DATALAKE","name":"STREAMING_OFFSET_AUDIT"},
       |"rowkey":"row",
       |"columns":{
       |"rowkey":{"cf":"rowkey","col":"row","type":"string"},
       |"countoffset":{"cf":"audit","col":"countoffset","type":"string"},
       |"date":{"cf":audit","col":"date","type":"string"}
       |}
       |}""".stripMargin
  }
}
