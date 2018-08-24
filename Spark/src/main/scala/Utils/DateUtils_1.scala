package Utils

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.commons.lang3.time.FastDateFormat




object DateUtils_1 {


  //预先存储的sdf
  lazy val yMd = new SimpleDateFormat("yyyyMMdd")
  lazy val yMdHms = new SimpleDateFormat("yyyyMMdd HHmmss")
  //undefined 变量为了保证只有一个 SimpleDateFormat 对象
  var undefined_1 :SimpleDateFormat = null
  var undefined_2 :SimpleDateFormat = null
  //线程安全对象  优先使用FastDateFormat
  var fdf: FastDateFormat = null
  //自定义日期格式
  private var sdf : SimpleDateFormat = null



  /**
    * 通过日期格式获取 simpleDateFormat对象
    * @param dateType
    * @return
    */
  def getSdf(dateType:String):SimpleDateFormat = {
    if(sdf == null){
      sdf = new SimpleDateFormat(dateType)
    }
    sdf
  }

  /**
    * 创建一个线程安全的时间解析对象
    */
  def getSafeInstance(dateType:String):FastDateFormat = {
    if(fdf == null){
      fdf = FastDateFormat.getInstance(dateType)
    }
    fdf
  }



  /**
    * 解析时间日期   日期格式样例: 10/Nov/2016:00:01:02 +0800    目标格式样例: yyyyMMddHHmmss
    */
  def parseDate(dateStr:String) :String = {
    if (undefined_2 == null){
      undefined_2 = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH )
    }

    try{
      yMdHms.format(undefined_2.parse(dateStr))
    } catch {
      case e:Exception => ""
    }
  }

}
