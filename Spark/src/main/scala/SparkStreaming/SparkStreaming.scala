package main.scala.SparkStreaming

import SparkStreaming.StreamingLogger
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming {

  def main(args: Array[String]): Unit = {
    StreamingLogger

    val conf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext( sc, Seconds(4) )

    sc.setCheckpointDir("E:\\temp\\sparkStreaming\\checkpoint")


    val kafkaParams = scala.collection.immutable.Map[String,String](
      "metadata.broker.list" -> "192.168.111.11:9092,192.168.111.12:9092",
      "auto.offset.reset" -> "largest",
      "group.id" -> "group01"
    )
    kafkaParams.foreach(println)


    val topicsSet :Set[String] = "topic01".split(",").toSet


    val value: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topicsSet)
    value.foreachRDD(rdd =>{
      rdd.for
    })



    ssc.start()
    ssc.awaitTermination()



  }




}
