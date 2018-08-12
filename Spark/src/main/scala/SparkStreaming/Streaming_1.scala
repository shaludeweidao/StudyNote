package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_1 {
  def main(args: Array[String]): Unit = {

    //初始化 StreamingContext, StreamingContext对象是所有的Spark Streaming功能的入口
    //StreamingContext对象 = SparkContext对象  +   时间间隔对象


    /***
      在定义一个 context 之后,您必须执行以下操作.
      通过创建输入 DStreams 来定义输入源.
      通过应用转换和输出操作 DStreams 定义流计算（streaming computations）.
      开始接收输入并且使用 streamingContext.start() 来处理数据.
      使用 streamingContext.awaitTermination() 等待处理被终止（手动或者由于任何错误）.
      使用 streamingContext.stop() 来手动的停止处理.

      需要记住的几点:
      一旦一个 context 已经启动，将不会有新的数据流的计算可以被创建或者添加到它。.
      一旦一个 context 已经停止，它不会被重新启动.
      同一时间内在 JVM 中只有一个 StreamingContext 可以被激活.
      在 StreamingContext 上的 stop() 同样也停止了 SparkContext 。为了只停止 StreamingContext ，设置 stop() 的可选参数，名叫 stopSparkContext 为 false.
      一个 SparkContext 就可以被重用以创建多个 StreamingContexts，只要前一个 StreamingContext 在下一个StreamingContext 被创建之前停止（不停止 SparkContext）.
      */
    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming_1")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))


    ssc.textFileStream("").flatMap(_.split(" "))
      .map(bean => (bean, 1))
      .reduceByKey(_ + _)


    //上面描述的是执行过程,如果要程序真正触发, 需要.start() 方法
    ssc.start()
    ssc.awaitTermination()








  }

}
