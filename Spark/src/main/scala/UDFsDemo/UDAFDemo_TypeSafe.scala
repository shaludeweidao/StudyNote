package UDFsDemo

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


//有问题,待解决

//Employee 输入数据的实体类
case class Employee(salary:Double,flag:Int)
//TempBean 中间结果的实体类
case class TempBean(var salary:Double, var count:Int, var max:Double, var flag:Int)


//注意: Aggregator位于org.apache.spark.sql.expressions.Aggregator这个包下面
object TypeSafeDemo extends Aggregator[Employee,TempBean,String]{

  //初始化方法
  override def zero: TempBean = TempBean(0.0, 0, 0.0, 0)

  //单个节点的计算方法
  override def reduce(b: TempBean, a: Employee): TempBean = {
    b.salary = b.salary + a.salary
    b.count += 1
    if (b.max < a.salary){
      b.max = a.salary
    }
    b.flag += a.flag

    //将结果对象写出
    b
  }

  //不同节点上的结果数据合并
  override def merge(b1: TempBean, b2: TempBean): TempBean = {
    b1.salary = b1.salary + b2.salary
    b1.count += b2.count
    if (b1.max < b2.max){
      b1.max = b2.max
    }
    b1.flag += b2.flag

    //将结果对象写出
    b1
  }

  //最终的结果计算
  override def finish(reduction: TempBean): String = {
    val salary = (reduction.salary - reduction.max) / (reduction.count - 1)
    val flag = reduction.flag
    ""+ salary + "_" + flag
  }



  //中间结果的序列化
  override def bufferEncoder: Encoder[TempBean] = Encoders.product

  //结果数据的序列化
  override def outputEncoder: Encoder[String] = Encoders.STRING
}