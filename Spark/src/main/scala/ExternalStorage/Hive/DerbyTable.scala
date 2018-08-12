package ExternalStorage.Hive

import org.apache.spark.sql.SparkSession


//通过加载本地文件映射成临时表
//    val sourceDF = spark.read.textFile("D:\\localFiles\\json.txt")
//    sourceDF.map(row => {
//      val temp: CesEntity = JSON.parseObject(row.toString().split(" - ")(1),classOf[CesEntity])
//      toutiaoBean(temp.getApp,temp.getAddTime,temp.getUpdTime,temp.getStatus,temp.getOst,     temp.getOst, temp.getOsv, temp.getUuid,temp.getIp,temp.getCallback,    temp.getImei,temp.getIdfa,temp.getChannel,temp.getAppid,temp.getAdid,    temp.getCid,temp.getMac)
//      //      tempBean(temp.getAddTime, temp.getAdid, temp.getApp, temp.getCallback, temp.getCid, temp.getImei, temp.getMac, temp.getOst+"", temp.getStatus+"", temp.getUpdTime, temp.getUuid, temp.getOsn, temp.getOsv, temp.getIp, temp.getIdfa, temp.getChannel, temp.getAppid)
//    }).createTempView("beanTable")

object DerbyTable {

  //构建sparkSession对象
  val spark = SparkSession.builder.master("local[2]").appName("DerbyTableTool").config("spark.sql.warehouse.dir","D:\\code58\\frank\\local").enableHiveSupport().getOrCreate


  def main(args: Array[String]): Unit = {
    //构建时间
//    var day: Date = new Date()
//    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd").format(day.getTime)
//    val yyyyMMdd_ = new SimpleDateFormat("yyyy-MM-dd").format(day.getTime)


    //建表语句sql
//    var sql_createTable = "CREATE TABLE if not exists hdp_ubu_wuxian_defaultdb.tb_app_action(\n`userid` STRING COMMENT'',\n`imei` STRING COMMENT'',\n`pid` STRING COMMENT'',\n`channelid` STRING COMMENT'',\n`idfa` STRING COMMENT'',\n`androidid` STRING COMMENT'',\n`appid` STRING COMMENT'',\n`uuid` STRING COMMENT'',\n`mac` STRING COMMENT'',\n`clientip` STRING COMMENT'',\n`timestamp` STRING COMMENT'',\n`cate1` STRING COMMENT'',\n`cate2` STRING COMMENT'',\n`cate3` STRING COMMENT'',\n`cate4` STRING COMMENT'',\n`city1` STRING COMMENT'',\n`city2` STRING COMMENT'',\n`city3` STRING COMMENT'',\n`pagetype` STRING COMMENT'',\n`actiontype` STRING COMMENT'',\n`operate` STRING COMMENT'',\n`request` STRING COMMENT'',\n`trackurl` STRING COMMENT'',\n`referrer` STRING COMMENT'',\n`wuxian_data` STRING COMMENT'',\n`ua` STRING COMMENT'',\n`version` STRING COMMENT'',\n`os` STRING COMMENT'',\n`osv` STRING COMMENT'',\n`apn` STRING COMMENT'',\n`uploadtime` STRING COMMENT'',\n`coord` STRING COMMENT'',\n`lat` STRING COMMENT'',\n`lon` STRING COMMENT'',\n`ltext` STRING COMMENT'',\n`resolution` STRING COMMENT'',\n`ak47` STRING COMMENT'',\n`params` STRING COMMENT'',\n`source` STRING COMMENT'',\n`idpool` STRING COMMENT'',\n`datapool` STRING COMMENT'',\n`backup` STRING COMMENT'',\n`p1` STRING COMMENT'',\n`p2` STRING COMMENT'',\n`p3` STRING COMMENT'',\n`p4` STRING COMMENT'',\n`p5` STRING COMMENT'',\n`p6` STRING COMMENT'',\n`p7` STRING COMMENT'',\n`p8` STRING COMMENT''\n ) COMMENT''\nPARTITIONED BY (`dt` INT COMMENT'') \nROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
    var sql_createTable = "CREATE TABLE IF not exists  hdp_teu_dpd_defaultdb.ds_umc_info(\n`userid` STRING,\n`register_name` STRING,\n`register_email` STRING,\n`register_mobile` STRING,\n`verified_mobile` DOUBLE,\n`verified_realname` DOUBLE,\n`verified_business` DOUBLE,\n`reg_time` STRING,\n`reg_ip` STRING,\n`reg_cityid` STRING,\n`reg_platform` STRING,\n`locked` DOUBLE,\n`md5_realname` STRING,\n`nick_name` STRING,\n`sex` INT,\n`birthday` STRING,\n`md5_qq` STRING,\n`md5_msn` STRING,\n`md5_mobile` STRING,\n`md5_phone` STRING,\n`postzip` STRING,\n`face` STRING,\n`verified_face` DOUBLE,\n`address` STRING,\n`aboutme` STRING,\n`work_locationid` STRING,\n`work_place` STRING,\n`living_locationid` STRING,\n`living_place` STRING,\n`hometownid` STRING,\n`agents` STRING,\n`extend` map<string, string>,\n`last_update` STRING,\n`hometownplace` STRING,\n`industry` STRING,\n`occupation` STRING,\n`company` STRING,\n`interests` STRING\n ) COMMENT''\nPARTITIONED BY (`ds` STRING) \nROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "

    //通过设置变量来   完成创建数据库,创建表,加载数据等等
    var partitionDate = "20180528"
    var databaseName = "hdp_teu_dpd_defaultdb"
    var tableName = "ds_umc_info"
    var partitionName = "ds"
    //    var partitionName = "stat_data"
    //注意点:  derby的目录一定要和加载数据的目录在一个盘符下,并且使用  /  这个方向的分隔符;   样例如下:
//    var inputPath = "/localFiles/tb_app_action/20180527"
    var inputPath = "/localFiles/ds_umc_info"

    //加载数据sql
//    var sql_loadData = "load data local inpath  \'" + inputPath +"\'  overwrite into table " + tableName +  "   partition (" + partitionName + " = " + partitionDate + "  )"
    var sql_loadData = "load data local inpath  \'" + inputPath +"\'  overwrite into table " + tableName +  "   partition (" + partitionName + " = 'umc'  )"



//    spark.read.text(inputPath).map(_.toString().split("\t")).createTempView("teampTable")
//    spark.sql("select * from tempTable").show()

    //创建database
//    spark.sql("create database if not exists  " + databaseName)
    //使用此数据库
//    spark.sql("use   " + databaseName)

    //==================个人使用区域_1


//    spark.sql("drop table  hdp_lbg_ecdata_dw_defaultdb.d_app_market_imei ")
//    spark.sql("select * from   hdp_lbg_ecdata_dw_defaultdb.d_app_market_imei ").show


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
