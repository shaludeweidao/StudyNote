package APIDemo

import org.apache.spark.sql.{Dataset, SparkSession}

object AggregateDemo {

  case class User(var department:String, var name:String, var salary:Double)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("AggregateDemo").getOrCreate()
    import spark.implicits._


    val ds: Dataset[User] = List(
      User("technical", "aa", 1.1),
      User("technical", "bb", 2.2),
      User("technical", "aa", 3.3),
      User("market", "a", 11.1),
      User("market", "b", 11.1),
      User("market", "c", 11.1),
      User("market", "b", 11.1),
      User("market", "a", 11.1)
    ).toDS()


    ds.map(bean =>{
      (  (bean.department, bean.name), bean.salary  )
    }).groupBy($"_1")
//      .groupByKey()








    spark.stop()
  }

}
