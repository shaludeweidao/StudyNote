package ProjectDemo.localHiveTools

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession


object DerbyTableTool {


  //构建时间
  var day: Date = new Date()
  private val yMd = new SimpleDateFormat("yyyyMMdd")
  private val yMd_ = new SimpleDateFormat("yyyy-MM-dd")
  lazy val today = yMd.format(day)
  lazy val today_ = yMd_.format(day)


  //设置必要变量    数据库名,表名,分区字段
  var databaseName = "hdp_ubu_wuxian_defaultdb"
  var tableName = "tb_app_action"
  var partitionFieldName = "dt"
  var partitionDate = "20180825"
  val fullTableName = s" ${databaseName}.${tableName}"


  def main(args: Array[String]): Unit = {


    //     .config("spark.sql.warehouse.dir","D:\\code58\\frank\\local")
    val spark = SparkSession.builder.master("local[3]").appName("DerbyTableTool").enableHiveSupport().getOrCreate
    import spark.implicits._


    val partitionFlag = "20180825"

    //查看hive中数据
    spark.sql(s"select * from ${fullTableName}  where ${partitionFieldName} = ${partitionFlag}").show(20,false)






    //查看表中的分区信息
    spark.sql(s"show  partitions").show(20,false)



    spark.stop()
  }


}
