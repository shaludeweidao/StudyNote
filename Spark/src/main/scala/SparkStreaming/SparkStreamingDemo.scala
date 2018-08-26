package SparkStreaming


import java.{lang, util}


import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingDemo {
  //构造 log日志的bean
  case class topicBean(imei:String, productorid:String, pushsource:String, n:String, date:String )

  def main(args: Array[String]): Unit = {
    //需要外界传入brokerList和topics参数
    if (args.length < 2){
      System.err.print("please set parameters: <brokers>,<topics> ")
      System.exit(1)
    }
    //获取brokerList,topics
    val Array(brokers, topics) = args

    //SparkConf是用来存储spark参数的类,底层存储在ConcurrentHashMap<String,String>中    一般最好将参数写在web页面的spark参数框中,这样可以动态更改参数值,避免硬编码
    val conf: SparkConf = new SparkConf()
      //一定要分配足够的线程数量,否则,会出现线程饿死现象
      .setMaster("local[2]")    //local[2]  表示本地运行,且分配2个线程
      .setAppName(this.getClass.getName)
      //使用Kryo序列化, 第一步设置序列化方式,第二步注册要序列化的类
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(topics.getClass))
    //通过创建SparkSession , SparkContext 和 StreamingContext
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //StreamingContext是sparkStreaming程序的入口       StreamingContext=SparkContext+时间对象
    val ssc: StreamingContext = new StreamingContext( sc , Seconds(6) )



    //从kafka读取数据, 最好采用direct模式
    //bug:  kafka.common.InvalidConfigException: Wrong value latest of auto.offset.reset in ConsumerConfig; Valid values are smallest and largest
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    val topicsSet = topics.split(",").toSet

    //kafka 0.8  写法
//    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream(ssc, kafkaParams, topicsSet)

    //kafka 0.10 写法
    val kafkaparams = new util.HashMap[String,Object]()
    val params: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(topicsSet,kafkaParams)[String,String]
    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferBrokers, params )
    val message: DStream[(String,String)] = value.map(message => {
      (message.key(),message.value())
    })





    //kafka数据获取之后     1.格式化数据   2.过滤脏数据    3.业务逻辑运算     4.结果数据写出到外部存储介质
    //.foreachRDD 此方法是将DStream对象转化为RDD   可以认为此部分代码就是在driver端运行的
    message.foreachRDD(rdd =>{

      //解析kafka日志,  其格式为(key,value)格式的数据,然而value才是kafka真正的数据
      val filterRDD: RDD[(Boolean, topicBean)] = rdd.flatMap { case (_1, log) => log.split("\n") }
        .map(log => {
          var flag:Boolean = false //flag 中 false表示删除状态,说明是脏数据,  true表示正常数据
          var bean: topicBean = null

          //此部分就是将log数据映射为javaBean的过程,如果不能映射成功,就认为是脏数据

          (flag, bean)
        }).filter( _._1 )


      //咱们部门一般会将数据向上汇总, 要使用flatMap算子,将一条数据变多条
      filterRDD.flatMap{
        case (flag, bean) =>{
          var beanArr = ArrayBuffer[(String,topicBean)]()
          //一条数据变多条数据逻辑代码...
          beanArr
        }
      }
      //此时的数据格式为  (imei, 维度+数据), 使用groupByKey进行imei聚合
      .groupByKey()
      .flatMap{
        case (imei, iter)=>{
          val arrBean = ArrayBuffer[( (String,String), (Int,Int,Int) )]()

          arrBean
        }
      }
      //使用aggregateByKey算子进行维度聚合
      .aggregateByKey( (0,0,0) )( seq _, seq _ )
      .foreachPartition( iter =>{
        //此部分代码是将结果数据写出

        //获取外部存储介质的连接对象,  例如:jedis链接
        //使用循环依次将结果写出,或者批量写出结果
        })
    })


    //SparkStreaming 程序启动是调用.start() 方法, 上面的代码都属于在描述运算逻辑
    ssc.start()
    //.awaitTermination() 方法表示程序等待外界停止
    ssc.awaitTermination()
  }


  //aggregateByKey的运算逻辑
  def seq(  a:(Int,Int,Int), b:(Int,Int,Int) ):(Int,Int,Int) ={
    val pv = a._1 + b._1
    val uv = a._2 + b._2
    val dayuv = a._3 + b._3
    (pv, uv, dayuv)
  }
}
