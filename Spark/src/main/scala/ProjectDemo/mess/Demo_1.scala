package ProjectDemo.mess

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//此类用于求留存的工具类
object Demo_1 {

  //构建mysql工具内部类
  class MySQLTool( url:String, user:String, password: String, tableName:String){

    //此方法先创建mysql表, 再删除历史数据
    def initMySQL(sql_create:String, sql_delete:String):Unit = {
      classOf[com.mysql.jdbc.Driver]
      val conn: Connection = DriverManager.getConnection(url,user,password)
      //创建mysql表,如果已经存在此表,会跳过
      conn.prepareStatement(sql_create).execute()
      //删除历史数据
      conn.prepareStatement(sql_delete)

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
      properties += ("createTableColumnTypes"->"stat_date date,product_id  varchar(30),p2   varchar(30) ,browser varchar(30),diaoqi_pv bigint, diaoqi_uv bigint, vppv bigint ")

      dataDF.write.options(properties).mode(SaveMode.Append).format("jdbc").save()

      println("write mysql success!!!")
    }
  }




  //实现hive的udtf的内部类
  private class UDTFDemo extends GenericUDTF{


    override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
      //注意一定要使用  java的ArrayList, 否则,ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType)报错

      //构建字段的名称
      val fieldName = new util.ArrayList[String]()
      fieldName.add("维度1")
      fieldName.add("维度2")
      fieldName.add("维度3")


      //构建字段的数据类型
      val fieldType = new util.ArrayList[ObjectInspector]()
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

      //将字段名称和字段类型输出
      return  ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType)
    }

    override def process(args: Array[AnyRef]): Unit = {

    }

    override def close(): Unit = {}
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
    if ( str == null || str.length == 0 || "null".equals(str.toLowerCase) || "-".equals(str)) { "other" }
    else{ str }
  }
  //判断字符串是否为数字
  def isNumber(value:String):Boolean = {
    try{ value.toInt; true }
    catch {case _:Exception => false}
  }




  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    //构建时间 变量   args(0) 传入的是运行时间的日期-1 ,并且格式为  yyyyMMdd
    println("args(0)=>" + args(0))
    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val yyyyMMdd_ = new SimpleDateFormat("yyyy-MM-dd")
    val dayPartition: String = args(0)
    val dayPartition_ = yyyyMMdd_.format(yyyyMMdd.parse(args(0)).getTime)
//    val dayPartitionBefore1: String = yyyyMMdd.format(yyyyMMdd.parse(args(0)).getTime - 86400000L)
//    val dayPartitionBefore1_ :  String  = yyyyMMdd_ .format(yyyyMMdd.parse(args(0)).getTime - 86400000L)



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
    spark.sql("set spark.sql.shuffle.partitions=500")


    //注册udf
    spark.udf.register("toInt",toInt _)
    spark.udf.register("toDouble",toDouble _)
    spark.udf.register("strJudge",strJudge _)
    spark.udf.register("isNumber",isNumber _)


    //加载hive表中的udtf,  一定要确保hive的udtf类的路径正确
    spark.sql("CREATE  TEMPORARY  FUNCTION  myUDTF  AS 'ProjectDemo.Demo_1.UDTFDemo'")


    val sql_getData =
      s"""
         |
       """.stripMargin
    val resourceData = spark.sql(sql_getData)
    resourceData.createOrReplaceTempView("resourceTable")


    //开始向上汇总
    val sql_collect =
      s"""
         |
       """.stripMargin
    val resultDF = spark.sql(sql_collect)



    val url = ""
    val user = ""
    val password = ""
    val tableName = ""
    val mysqlTool = new Demo_1.MySQLTool(url,user,password,tableName)

    val mysqlCreate =
      s"""
         |
       """.stripMargin
    val mysqlDelete =
      s"""
         |
       """.stripMargin
    //初始化mysql
    mysqlTool.initMySQL(mysqlCreate,mysqlDelete)

    //写数据到mysql
    mysqlTool.writeMySQL(resultDF)




    spark.stop()

    println("end...")
  }
}
