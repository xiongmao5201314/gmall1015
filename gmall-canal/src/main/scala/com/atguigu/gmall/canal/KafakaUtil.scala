package com.atguigu.gmall.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.internals.Topic

object KafakaUtil {
  val props = new Properties()
  props.put("bootstrap.servers","hadoop104:9092,hadoop106:9092,hadoop108:9092")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  def send(topic: String,content:String)={
      producer.send(new ProducerRecord[String,String](topic,content))
  }
}
