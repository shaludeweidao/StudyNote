package ProjectDemo.localHiveTools

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 一些常用的hive表
  *
  * hdp_ubu_wuxian_defaultdb.tb_app_action
  */

object LoadDataToHiveTool {

  //构建时间
  var day: Date = new Date()
  private val yMd = new SimpleDateFormat("yyyyMMdd")
  private val yMd_ = new SimpleDateFormat("yyyy-MM-dd")
  lazy val today = yMd.format(day)
  lazy val today_ = yMd_.format(day)


  //设置必要变量    数据库名,表名,分区字段
  var databaseName = "hdp_ubu_wuxian_defaultdb"
  var tableName = "tb_app_action"
  var partitionFieldName = "dt"
  var partitionDate = "20180825"
  val fullTableName = s" ${databaseName}.${tableName}"

  //注从集群中获取少量数据,并且加载到本地derby中,   本地下载的hive数据目录:  D:\loadDownHiveData
  def main(args: Array[String]): Unit = {
    //     .config("spark.sql.warehouse.dir","D:\\code58\\frank\\local")
    val spark = SparkSession.builder.master("local[3]").appName("DerbyTableTool").enableHiveSupport().getOrCreate
    spark.sql(s"use ${databaseName}")
//    spark.sql(s"desc ${fullTableName}").show(500,false)       //打印表结构信息



    val fatherPahtName = "20180825"  //数据源的父目录位置
    var inputPath = s"D:\\loadDownHiveData\\${tableName}\\" + fatherPahtName

    var map = scala.collection.mutable.Map[String,String]()
    map += ("header"->"true")       //是否有标题行
    map += ("delimiter"->"\t")      //分隔符，默认为逗号
    map += ("inferSchema"->"true")    //是否自动推到内容的类型
    val df = spark.read.options(map).csv(inputPath).cache()
    df.show( 50, false )   //打印数据
//    df.select("dt").show(500)


    //使用sql  动态插入数据
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")     //开启分区的非严格模式
    spark.sql("set hive.exec.dynamic.partition=true")    //启动动态分区插入数据
    df.createOrReplaceTempView("sourceDataTable")
    //动态插入数据     sql中的  partition()内放入  数据源表中已有的字段名称
    val sql_insert = s" insert overwrite table  ${fullTableName}   partition( dt )   select  *   from   sourceDataTable   "
//    spark.sql(sql_insert)       //真正执行sql去处理



    //使用代码的方式插入数据
    df.coalesce(1).write.partitionBy(s"${partitionFieldName}").mode(SaveMode.Overwrite).saveAsTable(s"${fullTableName}")




    //打印表分区字段
    spark.sql(s"show partitions ${fullTableName}").show(50,false)



    spark.stop()
  }


  //一些知识点

  //第1种类型            load本地数据
//追加      load data local inpath   '/jobcount/data'   into table dia_wormhole_cluster_pv     partition(type='track');
//覆盖       load data local inpath   '/jobcount/data'   overwrite into table dia_wormhole_cluster_pv     partition(type='track');

  //第2种类型            load   hdfs数据
//  load data inpath '/home/hdp_teu_dia/resultdata/zxl/input/2016/03/09' into table dia_wormhole_cluster_pv partition(type='track');
//  注：load data inpath是将数据move到表的存储位置下。分区表建议用add partition 的方式 添加数据，这样不会move数据

//  第3种类型            从别的表中查询出相应的数据并导入到Hive表中
//  3.1  追加数据
//  insert into table hdp_teu_dia_defaultdb.dia_wormhole_cluster_pv
//  select pageType, count(pageType) as total,dia_date from hdp_teu_dia_defaultdb.dia_wormhole_usdt_infolist_car_pc;
//  3.2  覆盖数据
//  insert overwrite table hdp_teu_dia_defaultdb.dia_wormhole_cluster_pv
//  select pageType, count(pageType) as total,dia_date         from           hdp_teu_dia_defaultdb.dia_wormhole_usdt_infolist_car_pc ;
//  3.3  覆盖分区表中分区的数据
//  insert overwrite   table dia_wormhole_cluster_pv        partition(type='track')
//  select pageType, count(pageType) as total,dia_date        from              hdp_teu_dia_defaultdb.dia_wormhole_usdt_infolist_car_pc  ;


}
