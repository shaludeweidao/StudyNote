package ProjectDemo

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.sql._


//此类用于求留存的工具类
object Test {

  //构建mysql工具内部类
  class MySQLTool( url:String, user:String, password: String, tableName:String){

    //此方法先创建mysql表, 再删除历史数据
    def initMySQL(sql_create:String, sql_delete:String):Unit = {
      classOf[com.mysql.jdbc.Driver]
      val conn: Connection = DriverManager.getConnection(url,user,password)
      //创建mysql表,如果已经存在此表,会跳过
      println(sql_create + "\n")
      conn.prepareStatement(sql_create).execute()
      //删除历史数据
      println(sql_delete + "\n")
      try{
        conn.prepareStatement(sql_delete).execute()
      }catch {
        case _:Exception => println("delete data fault")
      }


      conn.close()
      println("init mysql environment success!!!")
    }

    //写mysql函数
    def writeMySQL(dataDF:DataFrame, dbtable:String = tableName , url:String=url, user:String=user, password:String=password):Unit = {
      val properties = scala.collection.mutable.Map[String,String]()
      properties += ("url"->url)
      properties += ("driver"->"com.mysql.jdbc.Driver")
      properties += ("user"->user)
      properties += ("password"->password)
      properties += ("dbtable"->dbtable)

      //读和写的时候,  设置最大的连接mysql的数量,一个连接数产生一个分区,  当写mysql之前加入50个分区数据,然而自己设置的numPartitions=10,底层会自动调用coalesce(10)减少分区后写数据库
      properties += ("numPartitions"->"3")
      //写参数, 设置批量写mysql时,单批次写mysql的数据量,  默认1000条数据
      properties += ("batchsize"->"5000")
      //写参数, 当写模式设置为overwrite时,如果truncate设置为true,采用的是truncate table, 而不是drop table,之后在创建一张mysql表, 默认为false
      properties += ("truncate"->"true")
      //写参数, 用于设置mysql的生成表的执行引擎
      properties += ("createTableOptions"->" ENGINE=MyISAM DEFAULT CHARSET=utf8  ")
      //写参数, 用于设置字段的数据类型   其中_1,_2 都是DataSet中的字段名, 而后跟着的是mysql中的数据类型
      properties += ("createTableColumnTypes"->"stat_date date COMMENT '重试次数' , id  bigint, create_time  Datetime")

      dataDF.write.options(properties).mode(SaveMode.Append).format("jdbc").save()

      println("write mysql success!!!")
    }
  }








  //对数据进行转化的udf
  def toInt(str:String):Int = {
    try{ str.toInt }
    catch {case _:Exception => println(str); 0 }
  }
  def toDouble(str:String):Double = {
    try{ str.toDouble }
    catch {case _:Exception => println(str); 0.0}
  }
  //判断字符串是否为空
  def strJudge(str:String):String = {
    if ( str == null || str.length == 0 || str.contains(",") || "null".equals(str.toLowerCase) || "-".equals(str)) { "other" }
    else{ str }
  }
  //判断字符串是否为数字
  def isNumber(value:String):Boolean = {
    try{ value.toInt; true }
    catch {case _:Exception => false}
  }




  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()

    //构建时间 变量   args(0) 传入的是运行时间的日期-1 ,并且格式为  yyyyMMdd
    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val yyyyMMdd_ = new SimpleDateFormat("yyyy-MM-dd")
    val dayPartition: String = args(0)
    val dayPartition_ = yyyyMMdd_.format(yyyyMMdd.parse(args(0)).getTime)
//    val dayPartitionBefore1: String = yyyyMMdd.format(yyyyMMdd.parse(args(0)).getTime - 86400000L)
//    val dayPartitionBefore1_ :  String  = yyyyMMdd_ .format(yyyyMMdd.parse(args(0)).getTime - 86400000L)
    println( "args(0) =>" + args(0) )
    println( "dayPartition_ =>" + dayPartition_ )



    //设置优化
    // 设置为TRUE表示开启Spark的推测执行
    spark.sql("set spark.speculation=true")
    // 扫描到慢Task，执行时间是所有Task执行时间中值的1.4倍的时候启动另一个任务去并行运行这个Task
    spark.sql("set spark.speculation.multiplier=1.4 ")
    // 通过实际运行发现在没有发生数据倾斜的情况下，有些机器运行Task较慢，出现慢节点，把这个阈值设置的比较高，当的99%Task完成了才去扫描剩下没有完成的1%的Task
    spark.sql("set spark.speculation.quantile=0.90 ")
    spark.sql("set spark.network.timeout=600s")
    // 增加写磁盘的缓存
    spark.sql("set spark.shuffle.file.buffer=64k")
    spark.sql("set spark.shuffle.sort.bypassMergeThreshold=800")


    //设置shuffle数量
    spark.sql("set spark.sql.shuffle.partitions=10")


    //注册udf
    spark.udf.register("toInt",toInt _)
    spark.udf.register("toDouble",toDouble _)
    spark.udf.register("strJudge",strJudge _)
    spark.udf.register("isNumber",isNumber _)


    //加载hive表中的udtf,  一定要确保hive的udtf类的路径正确
    spark.sql("CREATE  TEMPORARY  FUNCTION  myUDTF  AS 'ProjectDemo.UDTFDemo'")


    val sql_getData =
      s"""
         |select
         |strJudge(`imei`) as imei ,
         |myUDTF ( strJudge(`product_id`),strJudge(`p2`) ,strJudge(`browser`) ) ,
         |strJudge(`diaoqi_pv`)  as diaoqi_pv,
         |strJudge(`vppv`) as vppv
         |from spark.userInfo
         |where stat_date='${dayPartition_}'
       """.stripMargin
    println(s"sql_getData =>\n${sql_getData}")
    val resourceData = spark.sql(sql_getData)
    resourceData.createOrReplaceTempView("resourceTable")
    spark.sql("select * from resourceTable").show(1000)


    //开始向上汇总
    //思路: 先根据 用户id及维度 进行group by 就是对各个维度下的用户去重复,   随后对 维度 进行group by, 求最终的结果
    val sql_collect =
      s"""
         |select
         |'${dayPartition_}' as stat_date,
         |product_id,
         |p2,
         |browser,
         |sum(diaoqi_pv) as diaoqi_pv,
         |sum(diaoqi_uv) as diaoqi_uv,
         |sum(vppv) as vppv
         |from
         |(
         |    select
         |    product_id,
         |    p2,
         |    browser,
         |    sum(diaoqi_pv) as diaoqi_pv,
         |    1 as diaoqi_uv,
         |    first_value(vppv) as vppv
         |    from  resourceTable
         |    group by  imei, product_id, p2, browser
         |) a
         |group by product_id, p2, browser
       """.stripMargin
    println(s"sql_collect is => \n${sql_collect}")
    val resultDF = spark.sql(sql_collect)
    resultDF.show(100)



    val url = "jdbc:mysql://localhost:3306/spark"
    val user = "root"
    val password = "root"
    val tableName = "ProductDemo"
    val mysqlCreate =
      s"""
         |CREATE TABLE if not exists  ${tableName} (
         |stat_date date comment '日期',
         |product_id varchar(30) DEFAULT 'no' comment '' ,
         |p2 varchar(30) DEFAULT 'no' comment '渠道号',
         |browser varchar(30) DEFAULT 'no' comment '浏览器',
         |diaoqi_pv bigint DEFAULT 0 comment '' ,
         |diaoqi_uv bigint DEFAULT 0 comment '' ,
         |vppv bigint  DEFAULT '0' COMMENT ''
         |) ENGINE=MyISAM DEFAULT CHARSET=utf8
       """.stripMargin
    val mysqlDelete = s"delete from ${tableName}  where stat_date = '${dayPartition_}' "
    val mysqlTool = new Test.MySQLTool(url,user,password,tableName)

    //初始化mysql
    mysqlTool.initMySQL(mysqlCreate,mysqlDelete)

    //写数据到mysql
    mysqlTool.writeMySQL(resultDF)




    spark.stop()

    println("end...")
  }
}


