package ExternalStorage.Hive

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


  // hdp_lbg_ecdata_dw_defaultdb.blockChainHistoryUser      hdp_ubu_wuxian_defaultdb.tb_app_action
  //设置必要变量    数据库名,表名,分区字段
  var databaseName = "hdp_lbg_ecdata_dw_defaultdb"
  var tableName = "blockChainHistoryUser"
//  var partitionFieldName = "dt"
//  var partitionDate = "20180825"
  val fullTableName = s" ${databaseName}.${tableName}"


  def main(args: Array[String]): Unit = {


    //     .config("spark.sql.warehouse.dir","D:\\code58\\frank\\local")
    val spark = SparkSession.builder.master("local[3]").appName("DerbyTableTool").enableHiveSupport().getOrCreate
    import spark.implicits._



    //查看hive中数据
    val partitionFlag = "20180825"
//    spark.sql(s"select * from ${fullTableName}  where ${partitionFieldName} = ${partitionFlag}").show(20,false)


    testFunction(spark)


    //查看表中的分区信息
    spark.sql(s"show   partitions   ${fullTableName} ").show(20,false)



    spark.stop()
  }


  //自定义
  def testFunction(spark: SparkSession): Unit ={
    val sql_show = s"select *  from ${fullTableName}  where dt = 20180827 "
    spark.sql(sql_show).show(30,false)

//    val sql_getAPPData =
//      s"""
//         |select  imei,        pagetype, actiontype, p3
//         |from hdp_ubu_wuxian_defaultdb.tb_app_action
//         |where dt = 20180826  and  imei is not null  and  actiontype is not null and ( pagetype = 'magicIsland'  or  (  pagetype = 'exchangei' and actiontype = 'inviteclick'  )  )
//       """.stripMargin
//    spark.sql(sql_getAPPData).show(30, false)
  }
}
