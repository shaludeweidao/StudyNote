package APIDemo

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ParseJson {
  case class JsonBean_scala(dt:String,key:String, value:String, meaning:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("ParseJson").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val listData = List(
      "{\"dt\":20180701,\"key\":\"k1\",\"value\":\"v1\",\"meaning\":\"The default number of partitions to use when shuffling data for joins or aggregations.\"}",
      "{\"dt\":20180701,\"key\":\"k1\",\"value\":\"v1\",\"meaning\":\"The default number of partitions to use when shuffling data for joins or aggregations.\"}",
      "{\"dt\":20180702,\"key\":\"k2\",\"value\":\"v2\",\"meaning\":\"The default data source to use in input/output.\"}"
    )

    val ds: Dataset[String] = listData.toDS()
    val jsonDS: Dataset[JsonBean_scala] = ds.map(bean => {
      try {
        //注: JSON在解析字符串的时候, 需要默认传入的类要自带默认构造器,也就是说,scala中的类的主构造器不能有参数
        val jsonBean = JSON.parseObject(bean, classOf[JsonBean_java])
        JsonBean_scala(jsonBean.dt,jsonBean.key, jsonBean.value, jsonBean.meaning)
      } catch {
        case _: Exception => {
          println("this is err => ", bean.toString)
          null
        }
      }
    })

    //将数据写入到 hive 表中
    spark.sql("create database if not exists spark")
    spark.sql("use spark")

    //spark 写hive数据库: 采用代码写
    //其中 partitionBy 指定的是分区字段名称  format 指定的是存储的数据格式类型
    jsonDS.coalesce(1).write.partitionBy("dt").format("json").mode(SaveMode.Append).saveAsTable("spark.parseJson_code")


    //spark 写hive数据库: 采用sql语句写
    jsonDS.createTempView("tempTable")
    val sql_createTable =
      s"""
         |create table if not exists spark.parseJson_sql(
         |key string COMMENT'',
         |value string comment'',
         |meaning string comment''
         |)comment'my table'
         |partitioned by (dt string)
         |row format delimited
         |fields terminated by '\t'
       """.stripMargin
    spark.sql(sql_createTable)
    //如果是动态插入分区数据, 需要关闭严格模式
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val sql_insertData =
      s"""
         |insert overwrite table  spark.parseJson_sql partition(dt='2018') select key,value,meaning  from tempTable
       """.stripMargin
    spark.sql(sql_insertData)
    spark.stop()

    println("success!!!")
  }
}
