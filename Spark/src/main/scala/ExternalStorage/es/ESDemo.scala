package ExternalStorage.es

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object ESDemo {
  def main(args: Array[String]): Unit = {


    val es_query =
      s"""
         |{
         |  "query":{
         |    "match_all": {}
         |  }
         |}
       """.stripMargin
    val conf = new SparkConf()
      .set("cluster.name", "ecdata-es")
      .set("es.nodes","10.126.123.194")
      .set("es.port","9200")
      .set("es.index.read.missing.as.empty","true")
      .set("es.nodes.wan.only","true")
      .set("es.resource","/i1/t1")
      .set("es.query",es_query)
    val spark = SparkSession.builder().appName("esdemo").master("local").config(conf).getOrCreate()

    EsSpark.esRDD(spark.sparkContext).collect().foreach(row =>{
      println(row)
    })


    spark.stop()

  }

}
