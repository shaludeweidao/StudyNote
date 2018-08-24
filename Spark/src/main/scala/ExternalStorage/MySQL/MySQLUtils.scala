package ExternalStorage.MySQL

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql._

object MySQLUtils{

  //本地测试环境
  var driver:String = "com.mysql.jdbc.Driver"
  var url:String = "jdbc:mysql://localhost:3306/spark"
  var user:String = "root"
  var password:String = "root"

  //生产环境
//  var driver:String = "com.mysql.jdbc.Driver"
//  var url:String = "jdbc:mysql://10.126.84.131:5029/data_dict"
//  var user:String = "task"
//  var password:String = "ecdata@0914"


  /**
    * 初始化MySQLUtils工具类
    * @param url
    * @param user
    * @param password
    */
  def init(url:String, user:String, password:String): Unit ={
    this.url = url
    this.user = user
    this.password = password
  }


  /**
    * 此方法用于初始化写mysql之前的环准备     1. 创建mysql表    2.删除之前的数据,一般按照日期删除
    */
  def writeTableBefore(createTableSQL:String, tableName:String, flag:String, url:String=this.url, user:String=this.user, password:String=this.password): Unit ={
    classOf[com.mysql.jdbc.Driver]
    val conn: Connection = DriverManager.getConnection(url,user,password)
    //创建表
    conn.prepareStatement(createTableSQL).execute()
    //删除历史数据
    val sql_deleteData = s"delete from ${tableName}  where stat_date=${flag} "
    conn.prepareStatement(sql_deleteData).execute()
    conn.close()
  }


  /**
    * 从mysql中读取数据
    * @param spark
    * @param dbtable
    * @param url
    * @param user
    * @param password
    * @return
    */
  def readMySQL(spark:SparkSession, dbtable:String , url:String=this.url, user:String=this.user,password:String=this.password):DataFrame = {
    val properties = scala.collection.mutable.Map[String,String]()
    properties += ("url"->url)
    properties += ("driver"->"com.mysql.jdbc.Driver")
    properties += ("user"->user)
    properties += ("password"->password)
    //dbtable参数就是获取的mysql表,不仅仅写成 确实存在的mysql表,还可以写成查询出来的临时表,如:  (select  a.*,b.*  from  a  left join b  on a.f=b.f )  resultTable
    properties += ("dbtable"->dbtable)
    //读和写的时候,  设置最大的连接mysql的数量,一个连接数产生一个分区,  当写mysql之前加入50个分区数据,然而自己设置的numPartitions=10,底层会自动调用coalesce(10)减少分区后写数据库
    properties += ("numPartitions"->"1")

    spark.read.options(properties).format("jdbc").load()
  }


  /**
    * 写数据到mysql
    * @param dataDF
    * @param dbtable
    * @param url
    * @param user
    * @param password
    */
  def writeMySQL(dataDF:DataFrame, dbtable:String,  url:String=this.url, user:String=this.user,password:String=this.password):Unit = {
    val properties = scala.collection.mutable.Map[String,String]()
    properties += ("url"->url)
    properties += ("driver"->this.driver)
    properties += ("user"->user)
    properties += ("password"->password)
    properties += ("dbtable"->dbtable)

    //读和写的时候,  设置最大的连接mysql的数量,一个连接数产生一个分区,  当写mysql之前加入50个分区数据,然而自己设置的numPartitions=10,底层会自动调用coalesce(10)减少分区后写数据库
    properties += ("numPartitions"->"1")
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




  //一些常用sql
  val createTableSQL =
    s"""
       |CREATE TABLE if not exists tableName (
       |stat_date date comment '统计日期',
       |field_1  varchar(50)  DEFAULT NULL  COMMENT '',
       |
       |field_2  TINYINT  DEFAULT '0'   COMMENT '',
       |field_3  int(5) DEFAULT NULL    COMMENT '',
       |field_4  bigint(20) DEFAULT 0   COMMENT '',
       |
       |field_5  double(20,4) DEFAULT '0.0000' COMMENT ''
       |) ENGINE=Brighthouse  DEFAULT 　CHARSET=utf8  COMMENT=''
     """.stripMargin
  //mysql执行引擎:　　　　Brighthouse　　　　MyISAM　　　　


}
