package ProjectDemo.localHiveTools

import org.apache.spark.sql.SparkSession

object CreateHiveTableTool {

  //设置必要变量    数据库名,表名,分区字段
  var databaseName = "hdp_ubu_wuxian_defaultdb"
  var tableName = "tb_app_action"
  var partitionFieldName = "dt"
  var partitionDate = "20180825"
  val fullTableName = s" ${databaseName}.${tableName}"


  //建表sql
  val sql_create =
    s"""
       |CREATE TABLE IF not exists  ${fullTableName}(
       |
       |)COMMENT ''
       |PARTITIONED BY ( `dt` int )
       |ROW FORMAT DELIMITED
       |  FIELDS TERMINATED BY '\001'
     """.stripMargin

  def main(args: Array[String]): Unit = {

    //     .config("spark.sql.warehouse.dir","D:\\code58\\frank\\local")
    val spark = SparkSession.builder.master("local[2]").appName("DerbyTableTool").enableHiveSupport().getOrCreate
    spark.sql(s"use ${databaseName} ")

    //建表创建hive表
    var sql_createTable =
      s"""
         |CREATE TABLE IF not exists  ${fullTableName}(
         |`userid` string,
         |  `imei` string,
         |  `pid` string,
         |  `channelid` string,
         |  `idfa` string,
         |  `androidid` string,
         |  `appid` string,
         |  `uuid` string,
         |  `mac` string,
         |  `clientip` string,
         |  `timestamp` string,
         |  `cate1` string,
         |  `cate2` string,
         |  `cate3` string,
         |  `cate4` string,
         |  `city1` string,
         |  `city2` string,
         |  `city3` string,
         |  `pagetype` string,
         |  `actiontype` string,
         |  `operate` string,
         |  `request` string,
         |  `trackurl` string,
         |  `referrer` string,
         |  `wuxian_data` string,
         |  `ua` string,
         |  `version` string,
         |  `os` string,
         |  `osv` string,
         |  `apn` string,
         |  `uploadtime` string,
         |  `coord` string,
         |  `lat` string,
         |  `lon` string,
         |  `ltext` string,
         |  `resolution` string,
         |  `ak47` string,
         |  `params` string,
         |  `source` string,
         |  `idpool` string,
         |  `datapool` string,
         |  `backup` string,
         |  `p1` string,
         |  `p2` string,
         |  `p3` string,
         |  `p4` string,
         |  `p5` string,
         |  `p6` string,
         |  `p7` string,
         |  `p8` string)
         |COMMENT ''
         |PARTITIONED BY (
         |  `dt` int)
         |ROW FORMAT DELIMITED
         |  FIELDS TERMINATED BY '\001'
       """.stripMargin
    spark.sql(sql_createTable)



    //删除hive表
//    spark.sql(s" drop table  if exists  ${fullTableName} ")


    //删除某个分区
//    spark.sql("alter table " + tableName + " drop if exists partition  ( dt=20180528)")



    //查看hive表的分区
    spark.sql(s"show partitions ${fullTableName}").show()




    spark.stop()
    println("end")
  }
}
