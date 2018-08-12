package UDFsDemo

import org.apache.spark.sql.SparkSession

object UDFDemo {

  def main(args: Array[String]): Unit = {

    //构建函数
    def udfDemo(department:String):String ={
      //获取前三位字母,并转化为大写
      department.substring(0,3).toUpperCase()
    }


    val spark = SparkSession.builder().master("local[2]").appName("UDFDemo").getOrCreate()
    import spark.implicits._


    List(
      ("1","technical",11.1,1),
      ("2","technical",22.2,1),
      ("3","technical",33.3,0),
      ("4","technical",44.4,1),
      ("5","technical",55.5,1),
      ("6","market",66.6,0),
      ("7","market",77.7,0),
      ("8","market",88.8,1),
      ("9","market",99.9,1)
    ).toDF("id","department","salary","workFlag").createOrReplaceTempView("userTable")


    //注册udf
    spark.udf.register("udfDemo",udfDemo _)
    spark.sql("select distinct udfDemo(department) as udfResult from userTable").show()









    spark.stop()


  }

}
