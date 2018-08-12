package UDFsDemo

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 支持集成现有 Hive 中的 UDF，UDAF 和 UDTF 的（Java或Scala）实现。
*UDTFs（user-defined table functions, 用户定义的表函数）可以返回多列和多行 - 它们超出了本文的讨论范围，我们可能会在以后进行说明。
*集成现有的 Hive UDF是非常有意义的，我们不需要向上面一样重新实现和注册他们。
*Hive 定义好的函数可以通过 HiveContext 来使用，
*不过我们需要通过 spark-submit 的 –jars 选项来指定包含 HIVE UDF 实现的 jar 包，然后通过 CREATE TEMPORARY FUNCTION 语句来定义函数


  hive中添加临时udf的语句:
  ADD JAR hdfs://hdp-58-cluster/home/hdp_58dp/udf/product-1.0-SNAPSHOT.jar;    //指定jar包位置
  CREATE TEMPORARY FUNCTION 函数别名 AS '包名.函数名';  //设置运行函数的别名

  转载: https://www.iteblog.com/archives/2038.html#Apache_SparkUDF
  */
object HiveUDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("HiveUDFDemo").enableHiveSupport().getOrCreate()
    import spark.implicits._


    //注册hive中的函数, 本例子采用 hive中的 udtf
    spark.sql("CREATE TEMPORARY FUNCTION myUDTF AS 'UDFsDemo.UDTFDemo_hive'")
    //显示所有的函数, 确认有没有完成自定义udtf的注册
    spark.sql("show functions ").show(200)

    List(
      ("aa","1","fa"),
      ("bbb","2","fb")
    ).toDF("f1","f2","f3")
      .createOrReplaceTempView("tempTable")

    //执行hive中的 udtf 函数
    //注意下面这种写法:   没有传入udtf中的字段,会自动追加到每一条记录中
    spark.sql("select myUDTF(f1,f2),f3 from tempTable").show()






    spark.stop()
  }

}
