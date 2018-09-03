package ExternalStorage.Hive

import org.apache.spark.sql.SparkSession

object CreateHiveTableTool {

  //设置必要变量    数据库名,表名,分区字段
  var databaseName = "hdp_ubu_wuxian_defaultdb"
  var tableName = "tb_app_action"
//  var partitionFieldName = "dt"
//  var partitionDate = "20180825"
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
         |  `cookieid` string COMMENT 'cookieId Nginx????? ???05dvOVC6Il6INhYABV6LAg==',
         |  `catepathid` string COMMENT '????Id????????',
         |  `catepath1` string COMMENT '??????????4',
         |  `catepath2` string COMMENT '??????????29',
         |  `catepath3` string COMMENT '??????????14052',
         |  `areapathid` string COMMENT '????id????????',
         |  `areapath1` string COMMENT '???????102',
         |  `areapath2` string COMMENT '???????123',
         |  `areapath3` string COMMENT '???????456',
         |  `clicktag` string COMMENT '?????????????from=XXXX',
         |  `url` string COMMENT '????????? url ??',
         |  `urlparams` map<string,string> COMMENT 'url???????',
         |  `clicktimestamp` string COMMENT '?????',
         |  `userip` string COMMENT '??????IP???175.153.41.6',
         |  `loginuid` string COMMENT '???? ID',
         |  `pagetype` string COMMENT '????',
         |  `page` string COMMENT '??????',
         |  `refdomain` string COMMENT 'referer??????????URL??',
         |  `referrer` string COMMENT 'referer URL????????URL',
         |  `referparams` map<string,string> COMMENT 'referrer url???????',
         |  `trackurl` map<string,string> COMMENT '??????  ??????_trackURL????',
         |  `otherparams` map<string,string> COMMENT '?????',
         |  `useragent` string COMMENT '????? UA ????????????????????',
         |  `browser` map<string,string> COMMENT '??????????????????????',
         |  `usertype` string COMMENT '????',
         |  `expandmessage` map<string,string> COMMENT '??????')
         |COMMENT 'ds层58、ganji、anjuke等ev(click)日志，属于dw层格式化规范数据源'
         |PARTITIONED BY (
         |  `statdate` string,
         |  `company` string,
         |  `platform` string,
         |  `biz` string,
         |  `pagetypepartition` string)
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
