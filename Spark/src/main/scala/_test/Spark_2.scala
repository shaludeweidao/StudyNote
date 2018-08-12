package _test

import org.apache.spark.sql.SparkSession

object Spark_2 {
  def main(args: Array[String]): Unit = {

    val date = "10/Nov/2016:00:01:02 +0800"

    val spark = SparkSession.builder().master("local[2]").appName("2").enableHiveSupport().getOrCreate()
    spark.sql("show databases").show()
    spark.sql("create database spark")
    spark.sql("show databases").show()


    spark.stop()



  }

}
