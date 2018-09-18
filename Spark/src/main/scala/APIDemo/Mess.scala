package APIDemo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object Mess {

  //在spark需要将 rdd 和 dataSet 进行相互转换的时候, 可以将case class写到内部, 作为内部类
  //case class 实现了 product 和 serializable 特质
  case class ab( name:String, age:Int )


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("1").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val broadcast: Broadcast[Map[Int, String]] = spark.sparkContext.broadcast(Map((1,"name1"), (2,"name2")))

    List(1,2,3).toDF.explain()

    val value: Dataset[(Int, String)] = List(
      (1, "a"), (2, "b"), (3, "c"), (1, "aa")
    ).toDS()


    value.mapPartitions(ite =>{
      val tempMap = broadcast.value
      ite.map(bean =>{
        val name = tempMap.getOrElse(bean._1,"other")
        (name,bean._2)
      })
    }).show()











    spark.stop()


  }

}
