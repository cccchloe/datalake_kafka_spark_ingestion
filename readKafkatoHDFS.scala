package com.marlabs.datalake

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}


object readKafkatoHDFS {

  case class records(topicName: String, partitionId: String, timeStamp: String, date: String, offset: Long, value: String, groupId: String)

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val logger = LogManager.getLogger(getClass.getName)

    if (args.size != 2) {
      System.err.println("please provide spark mode and confFile")
      logger.error("please provide spark mode and confFile")
      System.exit(1)
    }
    logger.info("input parameter is found")
    val mode = args(0).toString
    logger.info("mode is found")
    // val spark=CreateSession.getStreamingSession(mode,"")
    val confFilePath = args(1)
    val confFile = Paths.get(confFilePath).toFile
    val prop = ConfigFactory.parseFile(confFile)

    println("property for seconds is, " + prop.getString("incomingRawKafkaTopicReadDuration"))

    logger.info("@@@ Before spark streaming context")
    // create spark session
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    // Create Streaming Context and Kafka Direct Stream with provided settings and 10 seconds batches
    //val ssc = new StreamingContext(spark.sparkContext, Seconds(prop.getInt("incomingRawKafkaTopicRecordBatchTime")))
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    Try {
      logger.info("@@@ Entered try block")

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "earliest",
        "security.protocol" -> "PLAINTEXT",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
/**
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> prop.getString("bootstrap_servers"),
        "key.deserializer" -> prop.getString("key_deserializer"),
        "value.deserializer" -> prop.getString("value_deserializer"),
        "group.id" -> prop.getString("group_id"),
        "auto.offset.reset" -> prop.getString("auto_offset_reset"),
        "security.protocol" -> prop.getString("security_protocol"),
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val a_path = prop.getString("a_path")
      val b_path = prop.getString("b_path")

      //Function to get HDFSpath
      def pathCheck(HDFSCheck: String): String = {
        logger.info("@@@ path check " + HDFSCheck)
        val path = if (HDFSCheck == "A") {
          logger.info("@@@ A path")
          a_path
        } else {
          logger.info("@@@ b path")
          b_path
        }
        return path
      }
**/

      logger.info("@@@ keys " + kafkaParams.keys.mkString(","))
      logger.info("@@@ values " + kafkaParams.values.mkString(","))

      //create date format
      val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
      val dateFormate = new SimpleDateFormat("YYYYMMdd")

      //Create Dstream for each topic
      //val topics =spark.sparkContext.broadcast(prop.getString("incomingRawKafkaTopicName").split(","))
      val topics = Array("test" ,"test2")
      logger.info("@@@ topics" + topics)
      //val groupId = prop.getString("group_id")
      val groupId = "test"


      for (topic <- topics){
        logger.info("@@@ topic name " + topic)
        // read offset from hbase
        val kafkaOffsetDF = HbaseOffsetManagement.getKafkaOffsetDetails(spark)
        val fromOffsets = HbaseOffsetManagement.getFromOffsetDetails(kafkaOffsetDF)
        val len = fromOffsets.size
        logger.info("@@@ *************offset info******************")
        logger.info(s"@@@ size of the fromoffsets map for $topic is $len")
        logger.info("@@@ *************offset info end**************")

        val stream = if (fromOffsets != null && !fromOffsets.isEmpty){
          logger.info("@@@ creating stream with offsets")
          KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Assign[String,String](fromOffsets.keys, kafkaParams, fromOffsets)
          )
        }else{
          logger.info("@@@ creating stream without offsets")
          KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParams)
          )
        }
        logger.info("@@@ after stream creation")
        logger.info("@@@ present processing topic is $topic")

        import spark.implicits._

        // Parse each message and create Data Frame
        stream.foreachRDD(rdd => {
          logger.info("@@@ in side foreachRDD----" +rdd.count())
          if (!rdd.isEmpty()) {
            val timeStamp = System.currentTimeMillis().toString
            val dataFrameForMessage = rdd.map(record => {
              //records(topicName:String, partitionId:String,timeStamp:String, date:String,offset:Long,value:String,groupId:String)
              val current_time = dateFormat.format(record.timestamp())
              val offset = record.offset()
              val partitionId = record.partition().toString
              val topicName = record.topic().toString
              records(topicName, partitionId, timeStamp, current_time, offset, record.value(), groupId)
            }).toDF("topicName", "partitionId", "timeStamp", "date", "offset", "value", "groupId")
            logger.info("@@@ dataFrameForMessage created" + dataFrameForMessage.count())
            //dataFrameForMessage.orderBy($"offset".desc).show(100)


            // create dataframe for unique message IDs
            val dataFrameForID = rdd.map(record => record.key()).toDF()
            logger.info("@@@ dataFrameForID created" + dataFrameForID.count())


            // writing message to path, partition by message date
            logger.info("@@@ writing message to path")
            dataFrameForMessage
              .select(col("date"), col("value"))
              .write.partitionBy("date")
              .mode("append").format("text").save("output/")
            logger.info("@@@ successful")
            // writing message IDs to HDFS
            logger.info("@@@ writing message ID to path")
            dataFrameForID
              .coalesce(1).write
              .mode("append").format("text").save("output/messageID/")
            logger.info("@@@ successful")

            //commit offsets to kakfa
            logger.info("@@@ commit offsets for table")
            val rawOffsetDF=dataFrameForMessage
            HbaseOffsetManagement.updateKafkaOffsetDetails(rawOffsetDF)
            HbaseOffsetManagement.updateKafkaOffsetAudit(rawOffsetDF)

            /**
            val d1 = dataFrameForMessage.groupBy(col("partitionId").as("partition")).agg(max(col("offset")).as("maxOffset"))
            d1.show(false)
            val d2 = d1
              .select(col("partition"), col("maxOffset"))
              .join(dataFrameForMessage, (d1("partition") === dataFrameForMessage("partitionId") && (d1("maxOffset") === dataFrameForMessage("offset"))), "left")
            d2.show(false)


            //for audit table: count number of offsets each date
            val df2 = dataFrameForMessage
              .withColumn("rowkey", concat(col("topicName"), lit("~"), col("timeStamp"), lit("~"), col("groupId"), lit("~"), col("date")))
              .groupBy(col("rowkey"), col("date")).agg(count(col("offset")).as("countofOffset"))
              .select(col("rowkey"), col("date"), col("countofOffset"))

            df2.show()
            **/

            /**
             * val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
             *offsetRanges.foreach{offset =>
             * println(offset.topic.toString,offset.partition.toString,offset.untilOffset.toString,offset.fromOffset.toString)
             * val topicName=offset.topic.toString
             * val partition=offset.partition.toString
             * val untilOff=offset.untilOffset-1
             * val offsetDF=spark.sparkContext.parallelize(List((topicName,partition,untilOff)))
             * .toDF("topicName","partition","maxoffset")
             *
             **/
        }
      })
    }
    } match {
      case Success(k) => logger.info("@@@ Execution is success")
      case Failure(e) => logger.info("@@@ Error in consumer " + e.toString)
    }

    logger.info("@@@ after foreach RDD")
    ssc.start()
    ssc.awaitTermination()

    //ssc.stop()

  }
}