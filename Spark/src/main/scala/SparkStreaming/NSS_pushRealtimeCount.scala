package SparkStreaming

import java.util

import ExternalStorage.Redis.RedisUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer


object NSS_pushRealtimeCount {

  //构造 log日志bean  ,其中undefined 代表 imei 及 需求标记
  case class topicBean( undefined:String, productorid:String, pushsource:String, n:String, date:String )


  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.err.print("please set parameters: <brokers>,<topics> ")
      System.exit(1)
    }
    //获取brokerList,topics
    val Array(brokers, topics) = args
    println("brokers :" + brokers)
    println("topics :" + topics)



    //通过创建SparkSession , SparkContext 和 StreamingContext
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext( sc, Seconds(15) )



    //bug:  kafka.common.InvalidConfigException: Wrong value latest of auto.offset.reset in ConsumerConfig; Valid values are smallest and largest
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    val topicsSet = topics.split(",").toSet
    //使用direct模式读取kafka数据
    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)



    //解析log日志
    val filterData: DStream[(Int,topicBean)] = message.flatMap { case (_1, log) => log.split("\n") }
      .map(log => {
        var flag = -1  //flag 中 -1表示即将删除的状态,  3表示实时点击结果
        var bean : topicBean = null

        if ( true ) {
          flag = 3
          bean = topicBean( "imei", "platform", "p", "n", "201801010000" )
        } else {
          flag = 1
          bean = topicBean( "imei", "platform", "p", "n", "201801010000" )
        }

        (flag, bean)
      }).filter(item => item._1 != (-1) )


    filterData.foreachRDD(rdd =>{
      //此部分代码就是在driver端运行的

      //首先对每条数据进行向上汇总
      rdd.flatMap{
        case (_1, bean) =>{

          //beanArr的数据格式  (imei, 维度信息对象)
          var beanArr = ArrayBuffer[(String,topicBean)]()
          var undefined = ""  //此字段用来标记  不同的统计指标

          //根据不同的flag标记,进行不同的向上汇总
          if ( _1 == 1 ) {
            undefined = "plan"    //计划推送 pv
          } else if ( _1 == 2){
            undefined = "actual"  //实际推送 pv
          } else if( _1 == 3 ){
            undefined = "click"  //实际点击 pv, uv
          } else if ( _1 == 4){
            undefined = "limit"  //条数限制 pv, uv
          }

          //此时的 bean.undefined  保存的是  imei ,  undefined字段 即将存放的是 不同的统计指标: plan, actual, click, limit
          //productorid  不为 all
          beanArr.+=( (bean.undefined, topicBean( undefined, bean.productorid, bean.pushsource,"all",bean.date) ) )
          beanArr.+=( (bean.undefined, topicBean( undefined, bean.productorid, "all", "all",bean.date) ) )
          //productorid  为 all
          beanArr.+=( (bean.undefined, topicBean( undefined, "all", bean.pushsource, "all",bean.date)) )
          beanArr.+=( (bean.undefined, topicBean( undefined, "all", "all", "all",bean.date) ) )
          //n 不为 all
          if ("7".equals(bean.pushsource) || "11".equals(bean.pushsource) || "30".equals(bean.pushsource)){
            //满足此情况  n = "all"
          }else{
            //当pushsource 不是 7,11,30 情况下, 对 n 维度向上汇总
            beanArr.+=( (bean.undefined, topicBean( undefined, bean.productorid, bean.pushsource, bean.n, bean.date) )  )
            beanArr.+=( (bean.undefined, topicBean( undefined, "all", bean.pushsource, bean.n, bean.date) )  )
          }

          beanArr
        }
      }.groupByKey()
        .flatMap{
          case (imei, iter)=>{

            //  ( 维度,  (pv, uv) )  同时,uv如果为 -1 ,表示此结果不进行uv求和
            val arrBean = ArrayBuffer[(String, (Int,Int) )]()
            val hm = new util.HashMap[String, (Int,Int)]()

            //jedis链接
            val redisParams: Array[(String, Int)] = Array( ("host1",1111),("host2",2222) )
            lazy val jedisCluster: JedisCluster = RedisUtils.getJedisCluster(redisParams)

            for ( ite <- iter){
              //构造日期  yyyyMMddHHmm    = ite.date
              val yMd = ite.date.substring(0, 8)
              val Hm = ite.date.substring(8,11) + "0"

              //weidu 信息   redis中 rowkey设计 =>   push:[yyyyMMdd,yyyyMMddHHmm]:[plan,actual,click,limit]:platform:P:N:[pv,uv]
              val weidu = ite.undefined + ":" + ite.productorid + ":" + ite.pushsource + ":" + ite.n
              val weidu_yMd = yMd + ":" + weidu
              val weidu_yMdHm = yMd + Hm + ":" + weidu

              //此维度下是否存在此imei, 存在的话 ( pv+1, newuv+0)
              if ( hm.containsKey(weidu_yMdHm) ){
                val pv_uv: (Int,Int) = hm.get(weidu_yMdHm)

                hm.put(weidu_yMdHm, (pv_uv._1 + 1, pv_uv._2) )
                hm.put(weidu_yMd, (pv_uv._1 + 1, pv_uv._2) )
              } else {
                //此部分是 初次put到hashMap中, 需要检验newuv    在redis中检测是否存在此uv
                //redis缓存数据格式:    yyyyMMdd_imei  [各个维度]

                //判断天维度
                val flag_yMd = jedisCluster.sadd( yMd + ":" + imei, weidu )
                if (flag_yMd == 0) {
                  hm.put(weidu_yMd, (1,0))
                } else {
                  jedisCluster.expire( yMd + ":" + imei, 60*60*25 )
                  hm.put(weidu_yMd, (1,1))
                }

                //判断10分钟维度
                val flag_yMdHm = jedisCluster.sadd( yMd + ":" + imei , Hm + ":" + weidu )
                if (flag_yMdHm == 0){
                  hm.put(weidu_yMdHm,(1,0) )
                } else {
                  jedisCluster.expire( yMd + ":" + imei, 60*60*25 )
                  hm.put(weidu_yMdHm, (1,1))
                }
              }
            }

            //将数据保存到 beanArr
            val iterator = hm.entrySet().iterator()
            while (iterator.hasNext){
              val unit = iterator.next()
              arrBean += (unit.getKey -> unit.getValue)
            }
            arrBean
          }
        }.aggregateByKey( (0,0) )( seq _, seq _ )

        .foreachPartition( iter =>{

          //jedis链接
          val jedisCluster: JedisCluster = RedisUtils.getJedisCluster(null)


          while (iter.hasNext){
            val tuple:( String, (Int, Int) ) = iter.next()
            //推送统计只需要计算pv, 所以tuple._2._2 < 0 时, 表示 只需要将pv 数据写入redis
            if (tuple._2._2 < 0 ){
              //计划推送量,实际推送量  pv 写出
              jedisCluster.incrBy( "push:" + tuple._1 + ":pv", tuple._2._1)
            }else{
              //实际点击pv,uv   和 条数限制pv,uv 写出

              jedisCluster.incrBy( "push:" + tuple._1 + ":pv", tuple._2._1)
              jedisCluster.incrBy( "push:" + tuple._1 + ":uv", tuple._2._2)
            }
          }
        })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  
  //aggregateByKey
  def seq(  a:(Int,Int), b:(Int,Int) ):(Int,Int) ={
    val pv = a._1 + b._1
    val uv = a._2 + b._2
    (pv, uv)
  }
}
