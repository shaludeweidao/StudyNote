package ExternalStorage.Hive

import org.apache.spark.sql.SparkSession


//通过加载本地文件映射成临时表
//    val sourceDF = spark.read.textFile("D:\\localFiles\\json.txt")
//    sourceDF.map(row => {
//      val temp: CesEntity = JSON.parseObject(row.toString().split(" - ")(1),classOf[CesEntity])
//      toutiaoBean(temp.getApp,temp.getAddTime,temp.getUpdTime,temp.getStatus,temp.getOst,     temp.getOst, temp.getOsv, temp.getUuid,temp.getIp,temp.getCallback,    temp.getImei,temp.getIdfa,temp.getChannel,temp.getAppid,temp.getAdid,    temp.getCid,temp.getMac)
//      //      tempBean(temp.getAddTime, temp.getAdid, temp.getApp, temp.getCallback, temp.getCid, temp.getImei, temp.getMac, temp.getOst+"", temp.getStatus+"", temp.getUpdTime, temp.getUuid, temp.getOsn, temp.getOsv, temp.getIp, temp.getIdfa, temp.getChannel, temp.getAppid)
//    }).createTempView("beanTable")

object Test {

  //构建sparkSession对象
//  val spark = SparkSession.builder.master("local[2]").appName("Test").config("spark.sql.warehouse.dir","").enableHiveSupport().getOrCreate
  val spark = SparkSession.builder.master("local[2]").appName("Test").enableHiveSupport().getOrCreate


  def main(args: Array[String]): Unit = {
    //构建时间
//    var day: Date = new Date()
//    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd").format(day.getTime)
//    val yyyyMMdd_ = new SimpleDateFormat("yyyy-MM-dd").format(day.getTime)


    spark.sql("drop table if exists   spark.userInfo ")

    //建表语句sql
    var sql_createTable =
      s"""
         |CREATE TABLE if not exists spark.userInfo(
         |`imei` STRING,
         |`p2` STRING COMMENT'',
         |`browser` STRING COMMENT'',
         |`product_id` STRING COMMENT'',
         |`diaoqi_pv` STRING COMMENT'',
         |`vppv` STRING
         | ) COMMENT''
         |PARTITIONED BY (`stat_date` STRING)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       """.stripMargin
    spark.sql("create database if not exists spark")
    spark.sql("use spark")
    spark.sql(sql_createTable)






    //通过设置变量来   完成创建数据库,创建表,加载数据等等
    var partition = "2018-07-12"
    var databaseName = "spark"
    var tableName = "userInfo"
    var partitionName = "stat_date"
    //    var partitionName = "stat_data"
    //注意点:  derby的目录一定要和加载数据的目录在一个盘符下,并且使用  /  这个方向的分隔符;   样例如下:
    var inputPath = "e:/tmp/t1"
//    var inputPath = "/localFiles/ds_umc_info"

    //加载数据sql
//    var sql_loadData = "load data local inpath  \'" + inputPath +"\'  overwrite into table " + tableName +  "   partition (" + partitionName + " = " + partitionDate + "  )"
//    var sql_loadData = s"load data local inpath  \'" + inputPath +"\'  overwrite into table " + tableName +  "   partition (" + partitionName + " = '2018-07-12'  )"
    var sql_loadData = s"load data local inpath  \'" + inputPath +"\'  overwrite into table " + tableName +  "   partition (" + partitionName + s" = '${partition}'  )"
    spark.sql(sql_loadData)



//    spark.read.text(inputPath).map(_.toString().split("\t")).createTempView("teampTable")
//    spark.sql("select * from tempTable").show()

    //创建database
//    spark.sql("create database if not exists  " + databaseName)
    //使用此数据库
//    spark.sql("use   " + databaseName)

    //==================个人使用区域_1


//    spark.sql("drop table  hdp_lbg_ecdata_dw_defaultdb.d_app_market_imei ")
    spark.sql(s"select * from   ${databaseName}.${tableName} ").show


//    spark.sql("alter table " + tableName + " drop if exists partition  ( dt=20180528)")


    //========================= 终结  _1     =================================



    //创建table
//    spark.sql(sql_createTable)
    //加载数据
//    spark.sql(sql_loadData)
    //打印数据库中的表信息,并打印表中10条数据
//    printTables(databaseName,tableName,spark)

    println("")
    println("finish!!!")
    println("")

    //==================================个人使用区域


//    spark.sql("show partitions " + tableName).show()








    //==================================


    //关闭sparkSession
    spark.stop()
  }

  //打印数据库中的表信息
  def printTables(dataBase:String, tableName:String,spark:SparkSession): Unit ={
    spark.sql("use " + dataBase)
    spark.sql("show tables").show()
    spark.sql("select * from " + tableName).show(10)
  }

}
