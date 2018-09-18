package ProjectDemo.mess

import java.util

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}


//实现hive的udtf的内部类
class UDTFDemo   extends GenericUDTF {

    //构建维度数组
    val product_id = Array("","all")
    val p2 = Array("","all")
    val browser = Array("","all")


    override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
      //注意一定要使用  java的ArrayList, 否则,ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType)报错

      //构建字段的名称
      val fieldName = new util.ArrayList[String]()
      fieldName.add("product_id")
      fieldName.add("p2")
      fieldName.add("browser")



      //构建字段的数据类型
      val fieldType = new util.ArrayList[ObjectInspector]()
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

      //将字段名称和字段类型输出
      return  ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType)
    }

    override def process(args: Array[AnyRef]): Unit = {
      product_id(0) = args(0).toString
      p2(0) = args(1).toString
      browser(0) = args(2).toString

      for ( product_id <- product_id){
        for ( p2 <- p2){
          for ( browser <- browser){
            if ( "all".equals(product_id) ){
              if ( "all".equals(p2) ){
                forward(Array[String](product_id,p2,browser))
              }
            } else {
              forward(Array[String](product_id,p2,browser))
            }
          }
        }
      }
    }

    override def close(): Unit = {}
}
