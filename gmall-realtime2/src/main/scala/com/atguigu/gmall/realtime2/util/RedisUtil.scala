package com.atguigu.gmall.realtime2.util

import redis.clients.jedis.Jedis

object RedisUtil {
   val host: String = ConfigUtil.getProperty("redis.host")
   val port  = ConfigUtil.getProperty("redis.port").toInt

  def getClient()={
    val client = new Jedis(host, port, 60 * 1000)
    client.connect()
    client
  }
}
