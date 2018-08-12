package APIDemo

import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveModesDemo {
  def main(args: Array[String]): Unit = {

    /**
    Save Modes
    Save operations can optionally take a SaveMode, that specifies how to handle existing data if present. It is important to realize that these save modes do not utilize any locking and are not atomic. Additionally, when performing an Overwrite, the data will be deleted before writing out the new data.

    Scala/Java	Any Language	Meaning
    SaveMode.ErrorIfExists (default)	"error" or "errorifexists" (default)	When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
    SaveMode.Append	"append"	When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
    SaveMode.Overwrite	"overwrite"	Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
    SaveMode.Ignore	"ignore"	Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.

      Overwrite => 先将数据删除,之后将DataFrame数据写出
      Append => 将DataFrame数据追加到外部, 如果此表不存在,会首先创建表,之后再插入数据, 追加数据到外部数据, 不管这份数据是否和原先的数据相同
      Ignore => 将DataFrame数据追加到外部, 如果此表不存在,会首先创建表,之后再插入数据, 如果此表中有数据,会取消写操作

      */




    val spark = SparkSession.builder().master("local[2]").appName("SaveModesDemo").getOrCreate()
    import spark.implicits._


    val url:String="jdbc:mysql://localhost:3306/spark"
    val user:String="root"
    val password:String="root"
    val dbtable = "SaveModesDemo"
    val properties = scala.collection.mutable.Map[String,String]()
    properties += ("url"->url)
    properties += ("driver"->"com.mysql.jdbc.Driver")
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
    properties += ("createTableColumnTypes"->"name varchar(30), age tinyint , salary double  ")


    val dataDF = List(
//      ("frank", 1, 1.1),
//      ("yarn", 2, 2.2),
      ("spark", 3, 3.3),
      ("kafka",4,4.4)
    ).toDF("name", "age", "salary")

    dataDF.write.options(properties).mode(SaveMode.Ignore).format("jdbc").save()






    spark.stop()

    println("success")

  }

}
