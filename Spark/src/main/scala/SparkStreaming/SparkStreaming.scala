package main.scala.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext( sc, Seconds(3) )



    ssc.textFileStream("")














    ssc.start()
    ssc.awaitTermination()



  }




}
