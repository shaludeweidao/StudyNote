package main.scala.SparkStreaming

import ExternalStorage.mySQL.MySQLUtils
import Utils.SafeDateUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming {

  def main(args: Array[String]): Unit = {
    var flag = "yyyyMMdd"
    var directoryRDD:RDD[(String,String)] = null
    val dateUtils = SafeDateUtils.init("HHmm","")

    val conf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext( sc, Seconds(8) )

    sc.setCheckpointDir("E:\\temp\\sparkStreaming\\checkpoint")
    sc.setLogLevel("WARN")



    val kafkaParams = scala.collection.immutable.Map[String,String](
      "metadata.broker.list" -> "192.168.111.11:9092,192.168.111.12:9092",
      "auto.offset.reset" -> "largest",
      "group.id" -> "group01"
    )
    kafkaParams.foreach(println)


    val topicsSet :Set[String] = "topic01".split(",").toSet


    val value: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topicsSet)
    value.foreachRDD(rdd =>{
      val tempFlag = dateUtils.getCurrentDate(true)
      if (!flag.equals(tempFlag)){
        val directDF = MySQLUtils.readMySQL(spark,"jdbc")
        directoryRDD = directDF.rdd.map(row => (row(0)+"", row(1)+"") ).cache()
        flag = tempFlag
      }

      rdd.union( directoryRDD).collect.foreach(println)

      println("this is end")
    })



    ssc.start()
    ssc.awaitTermination()



  }




}
