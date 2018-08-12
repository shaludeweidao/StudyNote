package APIDemo

import org.apache.spark.sql.SparkSession

object Mess {

  //在spark需要将 rdd 和 dataSet 进行相互转换的时候, 可以将case class写到内部, 作为内部类
  //case class 实现了 product 和 serializable 特质
  case class ab( name:String, age:Int )


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("1").enableHiveSupport().getOrCreate()
    import spark.implicits._

    List(1,2,3).toDF.explain()

    val result  = List(
      (1, "a"), (2, "b"), (3, "c"), (1, "aa")
    ).toDS()











    spark.stop()


  }

}
