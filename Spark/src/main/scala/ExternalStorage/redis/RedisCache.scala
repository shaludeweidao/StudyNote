package ExternalStorage.redis

import java.{lang, util}

import redis.clients.jedis._

import scala.collection.mutable.ArrayBuffer


object RedisCache {


  private var jedisCluster: JedisCluster = null

  //获得redis连接对象
  def getJedisCluster(): JedisCluster  ={
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
      jedisClusterNodes.add(new HostAndPort("nss-realtime1.rdb.58dns.org", 5466 ))
      jedisClusterNodes.add(new HostAndPort("nss-realtime2.rdb.58dns.org", 5467 ))
      jedisClusterNodes.add(new HostAndPort("nss-realtime3.rdb.58dns.org", 5468 ))
      jedisClusterNodes.add(new HostAndPort("nss-realtime4.rdb.58dns.org", 5469 ))
      jedisClusterNodes.add(new HostAndPort("nss-realtime5.rdb.58dns.org", 5470 ))

      val timeout = 2000  //连接建立超时时间
      val maxAttempts = 5 //最多重定向次数

      jedisCluster = new JedisCluster(jedisClusterNodes, timeout, maxAttempts, config)
    }

    jedisCluster
  }

  def writeData_cluster (arr:ArrayBuffer[(String,Long)]){
    if (jedisCluster == null){
      getJedisCluster()
    }



  }


  //客户端连接
  def writeData_client(arr:ArrayBuffer[(String,Long)]){
    val config = new JedisPoolConfig()
    config.setMaxTotal(100)
    val jedis: Jedis = new JedisPool(config,"localhost", 6379, 3000  ).getResource

    arr.foreach( item =>{
      val boolean: lang.Boolean = jedis.exists(item._1)
      if (boolean){
        println("exists  " + item._1)
      }

      jedis.setex("xxx",20,"20seconds")
      jedis.incrBy(item._1, item._2)
    })

  }




  def main(args: Array[String]): Unit = {
    //客户端连接
    val config = new JedisPoolConfig()
    config.setMaxTotal(100)
    val jedis: Jedis = new JedisPool(config, "localhost", 6379, 3000  ).getResource
    println(jedis.get("frank"))
    jedis.incrBy("jedis", 22L)

    println("seccess")
  }

}
