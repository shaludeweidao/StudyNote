package Utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

//检测文件系统上是否存在某文件
object HadoopFileExist {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("tools").enableHiveSupport().getOrCreate()



    val bool = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path("e:\\aa.jar"))
    println(bool)




    spark.stop()



  }
}
