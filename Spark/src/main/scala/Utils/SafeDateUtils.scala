package Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat


//此类中   inputFormat格式的类型用 true表示,    outputFormat格式的类型用 false表示
class SafeDateUtils(val inputFormat:FastDateFormat, val outputFormat:FastDateFormat) extends Serializable {
  //按照inputFormat格式获取当天时间字符串   一般为:yyyyMMdd
  val today = inputFormat.format(new Date())

  /**
    *
    * @return
    */
  def getCurrentDate(flag:Boolean):String = {
    if (flag){
      inputFormat.format(new Date())
    }else{
      outputFormat.format(new Date())
    }

  }


  /**
    * 字符串 转化为 某天日期 String
    * @param days 相隔天数, 正数为传入日期之前的日期
    * @param inputFlag inputFlag == true  表示格式为inputFormat格式类型的,  否则 false 表示格式为 outputFomat格式类型的
    * @param inputDateString 需要解析的日期字符串
    */
  def getDateString(inputFlag:Boolean, days:Int = 0, outputFlag:Boolean = true, inputDateString:String = today) :String = {
    //根据传入的日期字符串进行解析
    val date: Date = {
      if (inputFlag){
        inputFormat.parse(inputDateString)
      }else{
        outputFormat.parse(inputDateString)
      }
    }
    if (outputFlag){
      inputFormat.format(date.getTime - ( 86400000L * days ) )
    }else{
      outputFormat.format(date.getTime - ( 86400000L * days ) )
    }
  }


  //将inputFormat格式的字符串  转化为  outputFormat格式的输出字符串
  def transformDateString(inputFlag:Boolean, inputDateString:String) :String ={
    getDateString( inputFlag, 0, !inputFlag, inputDateString )
  }

  /**
    * 日期字符串 转化为 某天日期 Date
    * @param inputDateString  传入的日期字符串
    * @param days 相隔天数, 正数为传入日期之前的日期
    */
  def getDate(inputFlag:Boolean, days:Int = 0, inputDateString:String = today) :Date = {
//    if (inputFlag){
//      new Date( inputFormat.parse(inputDateString).getTime - ( 86400000L * days ) )
//    }else{
//      new Date( outputFormat.parse(inputDateString).getTime - ( 86400000L * days ) )
//    }
    new Date( getDateLong(inputFlag, days, inputDateString) )
  }


  /**
    * 日期字符串 转化为 某天日期的 long类型
    * @param inputFlag
    * @param days
    * @param inputDateString
    * @return
    */
  def getDateLong(inputFlag:Boolean, days:Int = 0, inputDateString:String = today) :Long = {
    if (inputFlag){
      inputFormat.parse(inputDateString).getTime - ( 86400000L * days )
    }else{
      outputFormat.parse(inputDateString).getTime - ( 86400000L * days )
    }
  }



  //getFirstDayOfWeek
  @throws[Exception]
  def getFirstDayOfWeek(date: String): String = {
    val cal = Calendar.getInstance
    cal.setTime(inputFormat.parse(date))
    var d = 0
    val dayOfweek = cal.get(Calendar.DAY_OF_WEEK)
    if (1 == dayOfweek) d = -6
    else d = 2 - dayOfweek
    cal.add(Calendar.DAY_OF_WEEK, d)
    inputFormat.format(cal.getTime)
  }



  //getLastDayOfWeek
  @throws[Exception]
  def getLastDayOfWeek(date: String): String = {
    val cal = Calendar.getInstance
    cal.setTime(inputFormat.parse(date))
    var d = 0
    val dayOfweek = cal.get(Calendar.DAY_OF_WEEK)
    if (1 == dayOfweek) d = -6
    else d = 2 - dayOfweek
    cal.add(Calendar.DAY_OF_WEEK, d + 6)
    inputFormat.format(cal.getTime)
  }


  //getLastDayOfMonth
  @throws[Exception]
  def getLastDayOfMonth(dayStr: String): String = {
    val cal = Calendar.getInstance
    val date = inputFormat.parse(dayStr)
    cal.setTime(date)
    val value = cal.getActualMaximum(Calendar.DAY_OF_MONTH)
    cal.set(Calendar.DAY_OF_MONTH, value)
    inputFormat.format(cal.getTime)
  }

  //getFirstDayOfMonth
  @throws[Exception]
  def getFirstDayOfMonth(dayStr: String): String = {
    dayStr.substring(0,6) + "01"
  }

}


object SafeDateUtils extends Serializable {

  /**
    * 初始化DateUtils工具类,   为自己给定的时间格式
    */
  def init(input:String, output:String): SafeDateUtils ={
    synchronized{
      val inputFormat = FastDateFormat.getInstance(input)
      val outputFormat = FastDateFormat.getInstance(output)
      new SafeDateUtils(inputFormat,outputFormat)
    }
  }



}
