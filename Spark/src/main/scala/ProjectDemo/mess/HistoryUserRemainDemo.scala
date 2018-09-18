package ProjectDemo.mess

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql._


//统计历史用户的活跃,新增及留存率:
//0.创建历史累计用户hive表       表字段: imei, 活跃日期, 新增日期      注:此表每天更新的是历史所有用户的登陆信息
//1.从数据源中获取imei数据, 并且从历史表中获取历史数据        分别映射成临时表
//2.当天数据做基础表,历史数据表对其left join, 根据历史表中的日期做标记
//3.使用sum, 统计当天的活跃和新增总数
//4.统计历史表中的活跃和新增总数
//5.使用union all获取全量用户登陆信息,  之后写出最新的全量累计用户数据


object HistoryUserRemainDemo {
  //必要bean对象
//  case class Entity ( imei:String )

  def main(args: Array[String]): Unit = {
    //args判断
    var setInviteCount = 50
    if (args.length < 1){
      System.err.println("args is err")
      System.exit(1)
    } else if (args.length == 3){
      setInviteCount = args(1).toInt
    }
    println(s"args => ${args}")


    //构造时间参数
    val dateUtils = SafeDateUtils.init("yyyyMMdd" , "yyyy-MM-dd")
    val day = args(0)
    val day_ = dateUtils.transformDateString(true, day)
    val day1 = dateUtils.getDateString(true,1,true,day)
    println( s"days => ${day}  ${day_}  ${day1}" )


    //设置全局参数
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
//    spark.sql("set spark.sql.shuffle.partitions=800")
    //设置检查点
//    spark.sparkContext.setCheckpointDir("")



    //0 创建历史累计用户hive表
    val historyDataTableName = "hdp_lbg_ecdata_dw_defaultdb.blockChainHistoryUser"
    val historyDataPath = s"/home/hdp_lbg_ecdata_dw/warehouse/hdp_lbg_ecdata_dw_defaultdb/${historyDataTableName}/dt=" + day1
    val sql_createHistoryTable =
    s"""
       |CREATE TABLE if not exists ${historyDataTableName}(
       |imei STRING  COMMENT'',
       |actiondate  STRING COMMENT'活跃日期  yyyyMMdd',
       |newdate  STRING COMMENT'新增日期  yyyyMMdd'
       |)
       |COMMENT '挖矿用户累计表'
       |PARTITIONED BY (dt  STRING)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
       """.stripMargin
    spark.sql("use hdp_lbg_ecdata_dw_defaultdb")
    spark.sql(sql_createHistoryTable)
    //首先判断是否存在前一天数据
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val isExistsHistoryData: Boolean = fs.exists(new Path(historyDataPath))



    //给定初始值
    var actionUserCount:Long = 0L
    var actionUserCountRetention:Double =  0.0
    var newuserCount:Long =  0L
    var newuserCountRetention:Double =  0.0



    //1.从数据源中获取imei数据, 并且从历史表中获取历史数据        分别映射成临时表
    val currentTable = "currentTable"
    val historyTable = "historyTable"

    val sql_imeiData = s" select distinct imei  from  sourceTable  where  pagetype ='magicIsland' and actiontype = 'show' "
    val imeiDF = spark.sql(sql_imeiData).cache()
    imeiDF.createOrReplaceTempView(currentTable)


    //是否存在历史数据
    if(isExistsHistoryData){
      //2 根据全量累计用户表给当天imei做标记
      //标记当天imei的   actiondate,actionflag,   newdate, newflag  ( 活跃时间, 是否昨日活跃,  新增时间, 是否昨日新增 )
      //注: 此处 newdate 新增时间   是用来更新全量历史数据的
      val sql_doFlag = s"""
         |select
         |a.imei,
         |'${day}' as actiondate,
         |case when  b.actiondate = '${day1}'  then  1  else 0  end  as  actionflag,
         |case when  b.newdate is null  then  '${day}'  else  b.newdate   end as newdate,
         |case when  b.newdate = '${day1}'  then  1  else  0   end as newflag
         |from
         |${currentTable}  a
         |left join
         |( select  imei,actiondate,newdate  from  ${historyTable}   where  dt = '${day1}' )   b
         |on a.imei = b.imei
       """.stripMargin
      println(sql_doFlag)
      val imeiFlagDF = spark.sql(sql_doFlag).cache()
      val userFlagTable = "userFlagTable"
      imeiFlagDF.createOrReplaceTempView(userFlagTable)

      //3 统计当天的DAU和新增, 以及统计昨天的DAU和新增     ( actionUserCount,昨天活跃留存数量,  当天新增数量,昨天新增留存数量 )
      val sql_doCurrentCount =
        s"""
           |select
           |count(1) as actionUserCount ,
           |sum(actionflag) as remainaction ,
           |sum(case when newdate = '${day}' then 1 else 0 end ) as newuserCount,
           |sum(newflag) as remainnew
           |from  ${userFlagTable}
       """.stripMargin
      //数据统计
      val currentCountRow: Row = spark.sql(sql_doCurrentCount).collect()(0)


      //统计用户全量表中获取昨天的统计值     ( 昨天活跃数量, 昨天新增数量 )
      val sql_doHistoryCount =
        s"""
           |select
           |sum( case when actiondate = '${day1}' then 1 else 0 end ) as allaction,
           |sum( case when newdate = '${day1}' then 1 else 0 end ) as allnew
           |from ${historyDataTableName} where dt = '${day1}'
         """.stripMargin
      val historyCountRow: Row = spark.sql(sql_doHistoryCount).collect()(0)


      //将结果更新出去
      // ( actionUserCount,昨天活跃留存数量,昨天活跃数量,活跃留存率,   当天新增数量,昨天新增留存数量, 昨天新增数量, 新增留存率 )
      var actionUserCount_temp :Long = if( currentCountRow.getLong(0) == null ) 0L else currentCountRow.getLong(0)
      val yestedayUserRemainCount:Long = if ( currentCountRow.getLong(1) == null ) 0L else currentCountRow.getLong(1)
      val yestedayUserCount:Long = if ( historyCountRow.getLong(0) == null ) 1L else historyCountRow.getLong(0)
      var actionUserCountRetention_temp :Double =  1.0 * yestedayUserRemainCount / yestedayUserCount
      var newuserCount_temp :Long =  if( currentCountRow.getLong(2) == null ) 0L else currentCountRow.getLong(2)
      val yestedayNewUserRemainCount:Long = if ( currentCountRow.getLong(3) == null ) 0L else currentCountRow.getLong(3)
      val yestedayNewUserCount:Long = if ( historyCountRow.getLong(1) == null ) 1L else historyCountRow.getLong(1)
      var newuserCountRetention_temp : Double =  1.0 * yestedayNewUserRemainCount / yestedayNewUserCount

      actionUserCount = actionUserCount_temp
      actionUserCountRetention =  actionUserCountRetention_temp
      newuserCount =  newuserCount_temp
      newuserCountRetention =  newuserCountRetention_temp

      println(
        s"""
           |${actionUserCount_temp}    ${yestedayUserRemainCount}     ${yestedayUserCount}    ${actionUserCountRetention_temp}
           |${newuserCount_temp}    ${yestedayNewUserRemainCount}      ${yestedayNewUserCount}   ${newuserCountRetention_temp}
         """.stripMargin)



      //5 更新全量累计用户数据
      val sql_updateHistoryData =
      s"""
         |select imei, actiondate, newdate    from  ${userFlagTable}
         |union all
         |select  a.imei,a.actiondate, a.newdate
         |from
         |( select imei,actiondate,newdate  from ${historyDataTableName} where dt = '${day1}' )  a
         |left  join  imeiFlagTable   b
         |on a.imei = b.imei
         |where  b.imei  is  null
       """.stripMargin
      spark.sql(sql_updateHistoryData).repartition(8).createOrReplaceTempView("updateHistoryTable")
      val sql_writeHive = s"insert overwrite table ${historyDataTableName}  partition( dt ='${day}') select * from updateHistoryTable "
      spark.sql(sql_writeHive)
      println(s"写历史数据   ${sql_writeHive}")

    } else {
      //对于没有历史累计用户来说         1.统计数据指标        2.将当天imei用户写出
      //注: 对于第一天来说, 活跃和新增是相等的
      val actionCountAndNewCount: Long = imeiDF.count()
      actionUserCount = actionCountAndNewCount
      newuserCount = actionCountAndNewCount

      // 更新全量累计用户数据
      val sql_writeHive =  s"insert overwrite table hdp_lbg_ecdata_dw_defaultdb.blockChainHistoryUser  partition( dt ='${day}')  select imei,'${day}' as actiondate, '${day}' as newdate  from  currentTable "
      spark.sql("set spark.sql.shuffle.partitions=8")
      spark.sql(sql_writeHive)
      println(s"首次写历史数据     ${sql_writeHive}")
    }
    println(s"this is ( actionUserCount,昨天活跃留存数量,昨天活跃数量,活跃留存率,   当天新增数量,昨天新增留存数量, 昨天新增数量, 新增留存率 )  ")




    spark.stop()
    println("end")

  }
}
