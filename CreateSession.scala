package com.marlabs.datalake

import org.apache.spark.sql.SparkSession

object CreateSession {
  def getSession(mode: String, app: String): SparkSession = {
    return SparkSession
      .builder()
      .appName(app)
      .master(mode)
      .enableHiveSupport()
      .getOrCreate()
  }

  def getStreamingSession(mode: String, app: String): SparkSession = {
    return SparkSession
      .builder()
      .appName("pans-hive-kafka-streaming")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .config("spark.speculation", "true")
      .getOrCreate()
  }
}
