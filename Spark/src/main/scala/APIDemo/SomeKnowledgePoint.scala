package APIDemo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object SomeKnowledgePoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[2]").appName("SomeKnowledgePoint").getOrCreate()






    caseInfo(spark)



    spark.stop()
  }

  //构建数据
  def getDF(spark:SparkSession):DataFrame = {
    import spark.implicits._
    List(
      ("a",1),
      ("b",2),
      ("c",3),
      ("d",4)
    ).toDF("name","age")
  }


  //指定序列化类
  //比如:HBase中 org.apache.hadoop.hbase.io.ImmutableBytesWritable 和 org.apache.hadoop.hbase.client.Result 并没有实现 java.io.Serializable 接口
  //我们可以手动设置如何序列化ImmutableBytesWritable类，实现如下：
//  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//  sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  def serializableClass():SparkSession = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("serializable")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.getConf.registerAvroSchemas()


    spark
  }



  //使用case 匹配数据
  def caseInfo(spark:SparkSession):Unit = {
    import spark.implicits._

    val df: DataFrame = getDF(spark)
    df.map{
      //此处必须要和row的实体对象一一对应
      case Row(name:String, age:Int) => {
        s"name:, age:${age > 2}"
      }
    }.show()

  }



  //select
  def selectInfo(spark:SparkSession):Unit ={
    import spark.implicits._

    val df = getDF(spark)
    df.select($"name")
  }


}
