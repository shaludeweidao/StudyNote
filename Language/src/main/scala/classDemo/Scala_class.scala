package classDemo

import scala.beans.BeanProperty


//  private[包名],private[this] 可以放在字段，方法和类上，用来限制访问权限
//  1. private[包名]包名可以是父包名或当前包名，如果是父包名，则父包和子包都可以访问
//  2. private[this]修饰的方法或字段只能在本类访问，如果是字段编译成java的时候就没有get或set方法


//@BeanProperty 注解可以生成java的get和set方法

class Scala_class private[this] {

  @BeanProperty var name:String = null

}
