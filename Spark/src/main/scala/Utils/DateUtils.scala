package Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat


object DateUtils {

  //日期格式
  lazy val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
  lazy val today : String = yyyyMMdd.format( new Date() )


  var inputFormat :FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
  var outputFormat :FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")

  /**
    * 初始化DateUtils工具类,   为自己给定的时间格式
    */
  def init(input:String, output:String): Unit ={
    synchronized{
      inputFormat = FastDateFormat.getInstance(input)
      outputFormat = FastDateFormat.getInstance(output)
    }
  }



  /**
    * 字符串 转化为 某天日期 String
    * @param days 相隔天数, 正数为参照日期之前的日期
    * @param str 当前日期
    * @param flag 控制结果输出格式, 如果为true,表示outputFormat格式输出, 如果为false,表示inputFormat格式输出
    */
  def getDateString( days:Int = 0, flag :Boolean = true, str:String = today) :String = {
    if (flag){
      outputFormat.format( inputFormat.parse(str).getTime - ( 86400000L * days ) )
    }else{
      inputFormat.format( inputFormat.parse(str).getTime - ( 86400000L * days ) )
    }
  }


  //将字符串转化
  def transform(str:String) :String ={
    getDateString(0, true, str)
  }

  /**
    * 字符串 转化为 某天日期 Date
    * @param str
    * @param days
    */
  def getDate(str:String, days:Int = 0) :Date = {
    new Date( inputFormat.parse(str).getTime - ( 86400000L * days ) )
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
