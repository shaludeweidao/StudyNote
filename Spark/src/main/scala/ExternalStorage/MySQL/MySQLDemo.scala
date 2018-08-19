package ExternalStorage.MySQL

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql._

object MySQLDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ExternalStorage/MySQL").enableHiveSupport().getOrCreate()
    import spark.implicits._



//    val url = "jdbc:mysql://10.126.84.131:5029/data_dict"
//    val tableName = ""
//    val user = "task"
//    val password = "ecdata@0914"
//    val driver = "com.mysql.jdbc.Driver"




    //为了规定mysql中的数据格式,需要在driver端对mysql表进行人为创建,规定表格式,   同时删除历史任务的数据
    val url = "jdbc:mysql://localhost:3306/spark"
//    var url = "jdbc:mysql://localhost:3306/spark?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF-8"   //此url可以大幅提升写mysql的性能问题
    val user = "root"
    val password = "root"
    val tableName = "DriverCreateTable"
    val sql_mysqlTable =
      s"""
         |CREATE TABLE if not exists  ${tableName} (
         |name varchar(30) DEFAULT '0' comment 'name' ,
         |age tinyint DEFAULT 0 comment 'age' ,
         |salary double(20,4)  DEFAULT '0.0000' COMMENT 'salary'
         |) ENGINE=MyISAM DEFAULT CHARSET=utf8
       """.stripMargin
    val sql_deleteData = s"delete from ${tableName}  where name = 222 "
    classOf[com.mysql.jdbc.Driver]
    val conn: Connection = DriverManager.getConnection(url,user,password)
    //创建表
    conn.prepareStatement(sql_mysqlTable).execute()
    //删除历史数据
    conn.prepareStatement(sql_deleteData).execute()



//    //driver端批量写数据
//    val sql = "insert into tb_feature_rule_band (it_code,c_band) values (?,?) "
//    conn.setAutoCommit(false)
//    val pps: PreparedStatement = conn.prepareStatement(sql)
//    pps.clearBatch()
//    while(result.hasNext){
//      val item: (String, String) = result.next()
//      pps.setString(1,item._1)
//      pps.setString(2,item._2)
//      pps.addBatch()
//    }
//    pps.executeBatch()
//    conn.commit()

    conn.close()





    readMySQL(spark,"ab").show()


    //写mysql
    val df: DataFrame = List(
      ("n1", 1),
      ("n2", 2),
      ("n3", 3)
    ).toDF("name", "age")
//    writeMySQL(df,"mysqlDemo")



    spark.stop()


    println("success!!!")

  }


  def readMySQL(spark:SparkSession, dbtable:String , url:String="jdbc:mysql://localhost:3306/spark", user:String="root", password:String="root"):DataFrame = {

    val properties = scala.collection.mutable.Map[String,String]()
    properties += ("url"->url)
    properties += ("driver"->"com.mysql.jdbc.Driver")
    properties += ("user"->user)
    properties += ("password"->password)
    //dbtable参数就是获取的mysql表,不仅仅写成 确实存在的mysql表,还可以写成查询出来的临时表,如:  (select  a.*,b.*  from  a  left join b  on a.f=b.f )  resultTable
    properties += ("dbtable"->dbtable)
    //读和写的时候,  设置最大的连接mysql的数量,一个连接数产生一个分区,  当写mysql之前加入50个分区数据,然而自己设置的numPartitions=10,底层会自动调用coalesce(10)减少分区后写数据库
    properties += ("numPartitions"->"3")

    spark.read.options(properties).format("jdbc").load()
  }



  def writeMySQL(dataDF:DataFrame, dbtable:String , url:String="jdbc:mysql://localhost:3306/spark", user:String="root", password:String="root"):Unit = {

    val properties = scala.collection.mutable.Map[String,String]()
    properties += ("url"->url)
    properties += ("driver"->"com.mysql.jdbc.Driver")
    properties += ("user"->user)
    properties += ("password"->password)
    properties += ("dbtable"->dbtable)

    //读和写的时候,  设置最大的连接mysql的数量,一个连接数产生一个分区,  当写mysql之前加入50个分区数据,然而自己设置的numPartitions=10,底层会自动调用coalesce(10)减少分区后写数据库
    properties += ("numPartitions"->"3")
    //写参数, 设置批量写mysql时,单批次写mysql的数据量,  默认1000条数据
    properties += ("batchsize"->"1000")
    //写参数, 当写模式设置为overwrite时,如果truncate设置为true,采用的是truncate table, 而不是drop table,之后在创建一张mysql表, 默认为false
    properties += ("truncate"->"true")
    //写参数, 用于设置mysql的生成表的执行引擎
    properties += ("createTableOptions"->" ENGINE=MyISAM DEFAULT CHARSET=utf8  ")
    //写参数, 用于设置字段的数据类型   其中_1,_2 都是DataSet中的字段名, 而后跟着的是mysql中的数据类型
    properties += ("createTableColumnTypes"->"name varchar(30), age tinyint")

    dataDF.write.options(properties).mode(SaveMode.Overwrite).format("jdbc").save()
  }

}
