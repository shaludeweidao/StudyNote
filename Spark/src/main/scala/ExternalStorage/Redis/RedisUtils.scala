package ExternalStorage.Redis

import java.{lang, util}

import redis.clients.jedis._

import scala.collection.mutable.ArrayBuffer


//此方式是集群模式
object RedisUtils {

  //redis 连接工具
  private var jedisCluster: JedisCluster = null

  /**
    * 初始化redis连接对象
    * @param redisParams 存放redis连接参数  Array[("主机名", 端口号)]
    * @return
    */
  def getJedisCluster( redisParams:Array[(String,Int)] ): JedisCluster  ={
    if (jedisCluster == null){
      //数据库连接池配置
      val config: JedisPoolConfig = new JedisPoolConfig()
      //最大连接数
      config.setMaxTotal(100)
      //最大空闲连接数
      config.setMaxIdle(50)
      config.setMinIdle(20)
      config.setMaxWaitMillis(5000)
      config.setTestOnBorrow(false)
      config.setTestOnReturn(false)


      //Redis集群的节点集合
      val jedisClusterNodes: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort]()
      redisParams.map( item =>  jedisClusterNodes.add(new HostAndPort(item._1, item._2)))
//      jedisClusterNodes.add(new HostAndPort("nss-realtime1.rdb.58dns.org", 5466 ))
//      jedisClusterNodes.add(new HostAndPort("nss-realtime2.rdb.58dns.org", 5467 ))
//      jedisClusterNodes.add(new HostAndPort("nss-realtime3.rdb.58dns.org", 5468 ))
//      jedisClusterNodes.add(new HostAndPort("nss-realtime4.rdb.58dns.org", 5469 ))
//      jedisClusterNodes.add(new HostAndPort("nss-realtime5.rdb.58dns.org", 5470 ))

      val timeout = 2000  //连接建立超时时间
      val maxAttempts = 5 //最多重定向次数

      jedisCluster = new JedisCluster(jedisClusterNodes, timeout, maxAttempts, config)
    }

    jedisCluster
  }


  /**
    * 写数据到redis集群中
    * @param arr 装数据的容器
    */
  def writeData_cluster (arr:ArrayBuffer[(String,Long)]){
    if (jedisCluster == null){
      System.err.print("jedisCluster is null")
      System.exit(0)
    }
  }


  /**
    * 获取客户端连接对象
    */
  def getJedis( host:String, port:Int, password:String = "" ): Jedis = {
    // 数据库链接池配置
    val config: JedisPoolConfig = new JedisPoolConfig

    //最大连接数, 应用自己评估，不要超过Redis每个实例最大的连接数
    config.setMaxTotal(100)
    //最大空闲连接数, 应用自己评估，不要超过Redis每个实例最大的连接数
    config.setMaxIdle(50)
    config.setMinIdle(20)
    config.setMaxWaitMillis(6 * 1000)
    config.setTestOnBorrow(false)
    config.setTestOnReturn(false)

    //建立连接超时时间
    val timeout = 3000
    val jedisPool: JedisPool = new JedisPool(config, host, port, timeout, password)

    jedisPool.getResource
  }




  def main(args: Array[String]): Unit = {
    //客户端连接
    val jedis: Jedis = getJedis( "localhost", 6379 )
    println(jedis.get("frank"))
    jedis.incrBy("jedis", 22L)

    println("seccess")
  }

}
