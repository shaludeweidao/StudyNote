package collection

import scala.collection.mutable.ListBuffer


object CollectionDemo {

  def main(args: Array[String]): Unit = {


    var ints = ListBuffer(1,2,3)

    ints.+:(2)

    println(ints)
    println(ints.+:(2))





  }
}
