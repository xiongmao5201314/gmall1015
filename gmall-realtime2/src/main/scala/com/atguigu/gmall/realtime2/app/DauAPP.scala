package com.atguigu.gmall.realtime2.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime2.bean.StartupLog
import com.atguigu.gmall.realtime2.util.{ConfigUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object DauAPP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DauAPP")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)
    val startupLogStream = sourceStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartupLog]))
    //过滤后的数据
    val firstStartupLogStream = startupLogStream.transform(rdd => {
      val client = RedisUtil.getClient()
      val key = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val mids = client.smembers(key)
      client.close()
      val midsvalues = ssc.sparkContext.broadcast(mids)
      rdd.filter(log => !midsvalues.value.contains(log.mid))
        .map(log => {(log.mid,log)})
        .groupByKey()
        .map{
          case (_,it) => it.toList.minBy(_.ts)
        }


    })
    //把第一次启动的设备保存到redis中
    import org.apache.phoenix.spark._
    firstStartupLogStream.foreachRDD(rdd => {
      rdd.foreachPartition(logI =>{
        val client = RedisUtil.getClient()
        logI.foreach(log => {
          client.sadd(Constant.TOPIC_STARTUP+":"+log.logDate,log.mid)
        })
        client.close()
      })

      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop104,hadoop106,hadoop108:2181")
      )

    })
    firstStartupLogStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
