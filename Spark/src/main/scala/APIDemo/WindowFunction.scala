package APIDemo

import org.apache.spark.sql.SparkSession

//窗口sql的Demo

/**
  * row_number函数的用法：

（1）Spark 1.5.x版本以后，在Spark SQL和DataFrame中引入了开窗函数,其中比较常用的开窗函数就是row_number
     该函数的作用是根据表中字段进行分组，然后根据表中的字段排序；其实就是根据其排序顺序，给组中的每条记录添
     加一个序号；且每组的序号都是从1开始，可利用它的这个特性进行分组取top-n。
（2）row_number的格式：
    ROW_NUMBER() OVER (PARTITION BY 'xxx' ORDER BY 'xxx' DESC) rank
  */
object WindowFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("WindowFunction").getOrCreate()
    import spark.implicits._


    List(
      ("a","aa",1),
      ("b","bb",2),
      ("c","cc",3),
      ("b","bb",4),
      ("b","bb",44),
      ("a","aa",5)
    ).toDF("f1","f2","f3")
      .createOrReplaceTempView("tempTable")


    val sql_windowFunction =
      s"""
         |select
         |f1,f3
         |from
         |(
         |select f1,f3,row_number() over( partition by f1 order by f3 desc ) rank
         |from tempTable
         |) temp
         |where rank = 1
       """.stripMargin
//    spark.sql(sql_windowFunction).show()

    //准确上就是增加了 row_number() over(partition by xxx order by xxx ) as rank
    val sql_1 =
      s"""
         |select f1,f2,f3,row_number() over(partition by f1 sort by f3 desc ) as rank
         |from tempTable
         |order by f2
       """.stripMargin
    spark.sql(sql_1).show()
















    spark.stop()
  }

}
