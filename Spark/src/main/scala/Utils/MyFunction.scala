package Utils

object MyFunction {

  //字符串类操作

  /**
    * 字符串是否为空
    * @param str
    * @return
    */
  def isEmpty(str: String): Boolean = { if (str == null || str.length <= 0) true else false }
  /**
    * 字符串是否为数字
    * @param str
    * @return
    */
  def isNumber(str: String) : Boolean = {
    try{str.toLong; true }
    catch { case _:Exception => false }
  }
  /**
    * 将字符串转化为 int
    * @param default 如果转化失败,就返回 自定义的值
    * @return
    */
  def toInt(str:String, default:Int = 0 ):Int = {
    try{ str.toInt }
    catch {case _:Exception => default }
  }
  /**
    * 将字符串转化为 double
    * @param default 如果转化失败,就返回 自定义的值
    * @return
    */
  def toDouble(str:String, default:Double = 0.0 ):Double = {
    try{ str.toDouble }
    catch {case _:Exception => default }
  }






}
