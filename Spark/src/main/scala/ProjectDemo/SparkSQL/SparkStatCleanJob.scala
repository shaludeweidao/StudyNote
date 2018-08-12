package ProjectDemo.SparkSQL

import org.apache.spark.sql.SparkSession

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkStatCleanJob").getOrCreate()
    import spark.implicits._

    val resource: Range.Inclusive = 1.to(10)
//    val rdd = spark.sparkContext.textFile("")
    val rdd = spark.sparkContext.parallelize(resource).map( ("a", _))

    rdd.toDS().printSchema()
    rdd.toDS().show(false)

    "".substring(1)



    spark.stop()
  }

}
