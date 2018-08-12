package _test

import java.text.SimpleDateFormat

import ProjectDemo.Test
import org.apache.spark.sql.SparkSession

object Spark_1 {
  def main(args: Array[String]): Unit = {

    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val yyyyMMdd_ = new SimpleDateFormat("yyyy-MM-dd")
    val dayPartition: String = "20180404"
    val dayPartition_ = yyyyMMdd_.format(yyyyMMdd.parse(dayPartition).getTime)
//    val dayPartitionBefore1: String = yyyyMMdd.format(yyyyMMdd.parse(args(0)).getTime - 86400000L)
//    val dayPartitionBefore1_ :  String  = yyyyMMdd_ .format(yyyyMMdd.parse(args(0)).getTime - 86400000L)
    println("args(0)=>" + dayPartition)

//    println("{\"utm_source\":\"link\",\"spm\":\"b-31580022738699-me-f-906.LMZY_BannerWL_BJ_0722\"}".contains(","))


    val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val value = spark.sparkContext.broadcast(format)



    List(
      ("1","2018-02-22"),
      ("a","2018-07-03 17:34:16.0"),
      ("3","2018-07-03 17:34:16.0")
    ).toDF("id","create_time").createOrReplaceTempView("grade_tempTable")





    //写 mysql
    def toInt(str:String):Int = {
      try{ str.toInt }
      catch {case _:Exception => println(str); -1 }
    }
    def toLong(str:String):Long = {
      try{ str.toLong }
      catch {case _:Exception => println(str); -1L }
    }
    //判断字符串是否为空
    def strJudge(str:String, length:Int):String = {
      if ( str == null || str.length == 0 || str.length > length ) { "" }
      else{ str }
    }
    def isTime(str:String):String = {
      val format = value.value
      try{
        format.format(format.parse(str))
      }catch {
        case _:Exception => "2222-02-02"
      }
    }
    spark.udf.register("toInt", toInt _)
    spark.udf.register("toLong", toLong _)
    spark.udf.register("strJudge", strJudge _)
    spark.udf.register("isTime", isTime _)







    val sql_getMySQLData =
      s"""
         |select
         |'${dayPartition_}' as stat_date,
         |toLong(id) id ,
         |isTime(create_time)  create_time
         |from  grade_tempTable
      """.stripMargin
    val mySQLData = spark.sql(sql_getMySQLData).cache()
    mySQLData.show(10)




    val tableName = "t_tongzhen_grade_task_" + dayPartition_.substring(0,4)
    val url = "jdbc:mysql://localhost:3306/spark"
    val user = "root"
    val password = "root"


    val mysqlCreate =
      s"""
         |CREATE TABLE if not exists  ${tableName} (
         |stat_date date comment '日期',
         |id BIGINT comment '主键id',
         |create_time  Datetime   comment '创建时间'
         |)
         |ENGINE=MyISAM   DEFAULT CHARSET=utf8
         |COMMENT '同镇站长等级任务离线数据'
       """.stripMargin
    val mysqlDelete = s"delete from ${tableName}  where stat_date = '${dayPartition_}' "

    val tool = new Test.MySQLTool(url,user,password,tableName)
    tool.initMySQL(mysqlCreate,mysqlDelete)


    tool.writeMySQL(mySQLData,tableName,url,user,password)

    println("end")




  }

}
