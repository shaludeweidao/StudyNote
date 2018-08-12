package UDFsDemo

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * 用户自定义udaf的方式有两种,此方式是数据类型不安全的
  */
object UDAFDemo_TypeUnsafe {

  //此内部类实现了UDAF
  private class UDAFDemo extends UserDefinedAggregateFunction {
    //去掉最大值后求平均工资,打卡记录
    //原数据格式:   (员工编号, 部门id, 工资, 打卡标记)


    //输入数据的数据类型
    override def inputSchema: StructType = StructType{
      StructField("salary",DoubleType) ::
        StructField("flag",IntegerType) ::
        Nil
    }

    //中间数据的数据类型
    override def bufferSchema: StructType = StructType{
      StructField("salary",DoubleType) ::
        StructField("count",IntegerType) ::
        StructField("number",DoubleType) ::
        StructField("flag",IntegerType) ::
        Nil
    }

    //结果数据的数据类型
    override def dataType: DataType = StringType


    //输出数据是否为null
    override def deterministic: Boolean = false


    //初始化方法
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //初始化每个人的工资值
      buffer.update(0, 0.0)
      //初始化count
      buffer.update(1,0)
      //初始化工资最大值为0
      buffer.update(2, 0.0)
      //初始化每个部门的出勤人数
      buffer.update(3, 0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //求salary总数
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getInt(1) + 1
      //求工资最大值
      if (buffer.getDouble(2) < input.getDouble(0)){
        buffer(2) = input.getDouble(0)
      }
      //求出勤人数
      buffer(3) = buffer.getInt(3) + input.getInt(1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //求salary总数
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
      //求工资最大值
      if (buffer1.getDouble(2) < buffer2.getDouble(0)){
        buffer1(2) = buffer2.getDouble(2)
      }
      //求出勤人数
      buffer1(3) = buffer1.getInt(3) + buffer2.getInt(3)
    }

    //结果输出
    override def evaluate(buffer: Row): String = {
      val salary = ( buffer.getDouble(0) - buffer.getDouble(2) ) / (buffer.getInt(1) - 1 )
      "" + salary + "_" + buffer.getInt(3)
    }
  }




  //注册udaf方法
  def untypedDemo(spark:SparkSession):Unit = {
    //注:  参数1为udaf的名称, 参数2需要传入实现了UserDefinedAggregateFunction特质的对象,如果采用的是类的话,需要new一个对象作为参数,如果是object方式可以直接使用
    spark.udf.register( "myUDAF", new UDAFDemo() )
    val sql =
      s"""
         |select department,myUDAF(salary,workFlag) as result
         |from employeeTable
         |group by department
       """.stripMargin
    spark.sql(sql).show()
  }





  //测试
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("UDAFTest").getOrCreate()
    import spark.implicits._

    List(
      ("1","technical",11.1,1),
      ("2","technical",22.2,1),
      ("3","technical",33.3,0),
      ("4","technical",44.4,1),
      ("5","technical",55.5,1),
      ("6","market",66.6,0),
      ("7","market",77.7,0),
      ("8","market",88.8,1),
      ("9","market",99.9,1)
    ).toDF("id","department","salary","workFlag")
      .createOrReplaceTempView("employeeTable")

    //运行udaf结果
    untypedDemo(spark)


    spark.stop()
  }

}
