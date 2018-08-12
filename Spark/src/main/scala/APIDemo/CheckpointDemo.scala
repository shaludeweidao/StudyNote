package APIDemo

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object CheckpointDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("CheckPoint").enableHiveSupport().getOrCreate()
    import spark.implicits._


    spark.sparkContext.setCheckpointDir("E:\\tmp\\checkpoint")

    val data :DataFrame = List(
      (1,"a"),
      (2,"b"),
      (3,"c")
    ).toDF("age","description")


    val value: Dataset[(String, Int)] = data.map(row => {
      ("test", row.getInt(0) * 2)
    }).cache()
    value.checkpoint()

    value.createOrReplaceTempView("tempTable")

    spark.sql("select * from tempTable").show()


    spark.sql("select * from tempTable").write.mode(SaveMode.Append)




    spark.stop()

  }
}
