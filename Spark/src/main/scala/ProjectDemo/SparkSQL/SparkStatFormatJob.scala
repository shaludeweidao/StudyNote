package ProjectDemo.SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("sparkStatFormatJob").getOrCreate()
    import spark.implicits._

    //    spark.read.textFile("file://")
        val resourceRDD: RDD[String] = spark.sparkContext.textFile("file://E:\\tmp\\201608.txt")
    resourceRDD.map(line =>{
      val words = line.split("\t")
      val first = words(0)
    }).take(10).foreach(println _)



    spark.stop()


  }
}
