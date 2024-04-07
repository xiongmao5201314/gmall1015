package com.atguigu.gmall.realtime2.util


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
  val params=Map[String,String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigUtil.getProperty("kafka.servers"),
    ConsumerConfig.GROUP_ID_CONFIG -> ConfigUtil.getProperty("kafka.group.id")
  )

  def getKafkaStream(ssc:StreamingContext,topic:String,otherTopics:String*)={
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      params,
      (otherTopics :+ topic).toSet
    ).map(_._2)
  }
}
